// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Columnar understanding of persisted data
//!
//! For efficiency/performance, we directly expose the columnar structure of
//! persist's internal encoding to users during encoding and decoding. For
//! ergonomics, we wrap the arrow2 crate we use to read and write parquet data.
//!
//! Some of the requirements that led to this design:
//! - Support a separation of data and schema because Row is not
//!   self-describing: e.g. a Datum::Null can be one of many possible column
//!   types. A RelationDesc is necessary to describe a Row schema.
//! - Narrow down arrow2::data_types::DataType (the arrow "logical" types) to a
//!   set we want to support in persist.
//! - Associate an arrow2::io::parquet::write::Encoding with each of those
//!   types.
//! - Do `dyn Any` downcasting of columns once per part, not once per update.
//! - Unlike arrow2, be precise about whether each column is optional or not.
//!
//! The primary presentation of this abstraction is a sealed trait [Data], which
//! is implemented for the owned version of each type of data that can be stored
//! in persist: `int64`, `Option<String>`, etc.
//!
//! Under the hood, it's necessary to store something like a map of `name ->
//! column`. A natural instinct is to make Data object safe, but I couldn't
//! figure out a way to make that work without severe limitations. As a result,
//! the DataType enum is introduced with a 1:1 relationship between variants and
//! implementations of Data. This allows for easy type erasure and guardrails
//! when downcasting the types back.
//!
//! Note: The "Data" strategy is roughly how columnation works and the
//! "DataType" strategy is roughly how arrow2 works. Doing both of them gets us
//! the benefits of both, while the downside is code duplication and cognitive
//! overhead.
//!
//! The Data trait has associated types for the exclusive "builder" type for the
//! column and for the shared "reader" type. These implement also implement some
//! common traits to make relationships between types more structured.
//!
//! Finally, the [Schema] trait maps an implementor of [Codec] to the underlying
//! column structure. It also provides a [PartEncoder] and [PartDecoder] for
//! amortizing any downcasting that does need to happen.

use std::fmt::Debug;

use crate::codec_impls::UnitSchema;
use crate::columnar::sealed::ColumnRef;
use crate::part::{ColumnsMut, ColumnsRef, PartBuilder};
use crate::Codec;

/// A type understood by persist.
///
/// The equality and sorting of the encoded column matches those of this rust
/// type.
///
/// This trait is implemented for owned types. However, for efficiency the
/// columns themselves don't store the owned types, so instead we read and write
/// in terms of the associated [Self::Ref]. This is not simply `&Self` because
/// e.g. it's sometimes not possible for us to present the column as something
/// like `&Option<T>` but we can always produce a `Option<&T>`. Tuples have a
/// similar restriction.
///
/// This trait is intentionally "sealed" via the unexported Column trait.
///
/// There is a 1:1 mapping between implementors of [Data] and variants of the
/// [DataType] enum. The parallel hierarchy exists so that Data can be ergonomic
/// while DataType is object-safe and has exhaustiveness checking. A Data impl
/// can be mapped to it's corresponding DataType via [Data::TYPE] and back via
/// DataType::data_fn.
pub trait Data: Debug + Send + Sync + Sized + 'static {
    /// The DataType variant corresponding to this data type.
    const TYPE: DataType;

    /// The associated reference type of [Self] used for reads and writes on
    /// columns of this type.
    ///
    /// TODO: We may want to eventually separate this into In and Out types
    /// because one wants to be covariant and the other wants to be
    /// contravariant.
    type Ref<'a>
    where
        Self: 'a;

    /// The shared reference of columns of this type of data.
    type Col: ColumnGet<Self> + From<Self::Mut>;

    /// The exclusive builder of columns of this type of data.
    type Mut: ColumnPush<Self> + Default;
}

/// A type that may be retrieved from a column of `[T]`.
pub trait ColumnGet<T: Data>: ColumnRef {
    /// Retrieves the value at index.
    fn get<'a>(&'a self, idx: usize) -> T::Ref<'a>;
}

/// A type that may be added into a column of `[T]`.
pub trait ColumnPush<T: Data>: Send + Sync {
    /// Pushes a new value into this column.
    fn push<'a>(&mut self, val: T::Ref<'a>);
}

pub(crate) mod sealed {
    use arrow2::array::Array;
    use arrow2::io::parquet::write::Encoding;

    /// A common trait implemented by all `Data::Col` types.
    pub trait ColumnRef: Sized + Send + Sync {
        /// Returns the number of elements in this column.
        fn len(&self) -> usize;

        /// Returns this column as an arrow2 Array.
        fn to_arrow(&self) -> (Encoding, Box<dyn Array>);

        /// Constructs the column from an arrow2 Array.
        #[allow(clippy::borrowed_box)]
        fn from_arrow(array: &Box<dyn Array>) -> Result<Self, String>;
    }
}

/// A description of a type understood by persist.
///
/// There is a 1:1 mapping between implementors of [Data] and variants of the
/// [DataType] enum. The parallel hierarchy exists so that Data can be ergonomic
/// while DataType is object-safe and has exhaustiveness checking. A Data impl
/// can be mapped to it's corresponding DataType via [Data::TYPE] and back via
/// DataType::data_fn.
#[derive(Debug, Clone, PartialEq)]
pub struct DataType {
    /// Whether this type is optional.
    pub optional: bool,
    /// The in-memory rust type of a column of data.
    pub format: ColumnFormat,
}

/// The in-memory rust type of a column of data.
///
/// The equality and sorting of the encoded column matches those of this rust
/// type. Because of this, the variants are named after the rust type.
///
/// NB: This intentionally exists as a subset of [arrow2::datatypes::DataType].
/// It also represents slightly different semantics. The arrow2 DataType always
/// indicates an optional field, where as these all indicate non-optional fields
/// (which may be made optional via [DataType]).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColumnFormat {
    /// A column of type [bool].
    Bool,
    /// A column of type [i8].
    I8,
    /// A column of type [i16].
    I16,
    /// A column of type [i32].
    I32,
    /// A column of type [i64].
    I64,
    /// A column of type [u8].
    U8,
    /// A column of type [u16].
    U16,
    /// A column of type [u32].
    U32,
    /// A column of type [u64].
    U64,
    /// A column of type [f32].
    F32,
    /// A column of type [f64].
    F64,
    /// A column of type [`Vec<u8>`].
    Bytes,
    /// A column of type [String].
    String,
    // TODO: FixedSizedBytes for UUIDs?
    // TODO: Struct?
}

/// An encoder for values of a fixed schema
///
/// This allows us to amortize the cost of downcasting columns into concrete
/// types.
pub trait PartEncoder<'a, T> {
    /// Encodes the given value into the Part being constructed.
    fn encode(&mut self, val: &T);
}

/// A decoder for values of a fixed schema.
///
/// This allows us to amortize the cost of downcasting columns into concrete
/// types.
pub trait PartDecoder<'a, T> {
    /// Decodes the value at the given index.
    ///
    /// Implementations of this should reuse allocations within the passed value
    /// whenever possible.
    fn decode(&self, idx: usize, val: &mut T);
}

/// A description of the structure of a [crate::Codec] implementor.
pub trait Schema<T>: Debug + Send + Sync {
    /// The associated [PartEncoder] implementor.
    type Encoder<'a>: PartEncoder<'a, T>;
    /// The associated [PartDecoder] implementor.
    type Decoder<'a>: PartDecoder<'a, T>;

    /// Returns the name and types of the columns in this type.
    ///
    /// TODO: This is the only place where DataType leaks externally. We could
    /// tighten up the abstraction by instead passing into this method some
    /// object with a method like `fn add<T: Data>(name: String)`. If we decide
    /// to support struct columns, a hypothetical StructSchemaBuilder would look
    /// very much like this. Decide if this is better/worth it.
    fn columns(&self) -> Vec<(String, DataType)>;

    /// Returns a [Self::Decoder<'a>] for the given columns.
    fn decoder<'a>(&self, cols: ColumnsRef<'a>) -> Result<Self::Decoder<'a>, String>;

    /// Returns a [Self::Encoder<'a>] for the given columns.
    fn encoder<'a>(&self, cols: ColumnsMut<'a>) -> Result<Self::Encoder<'a>, String>;
}

/// A helper for writing tests that validate that a piece of data roundtrips
/// through the columnar format.
pub fn validate_roundtrip<T: Codec + Default + PartialEq + Debug>(
    schema: &T::Schema,
    val: &T,
) -> Result<(), String> {
    let mut part = PartBuilder::new(schema, &UnitSchema);
    schema.encoder(part.key_mut())?.encode(val);
    part.push_ts_diff(1, 1);
    let part = part.finish()?;

    let mut actual = T::default();
    assert_eq!(part.len(), 1);
    let part = part.key_ref();
    schema.decoder(part)?.decode(0, &mut actual);
    if &actual != val {
        Err(format!(
            "validate_roundtrip expected {:?} but got {:?}",
            val, actual
        ))
    } else {
        Ok(())
    }
}
