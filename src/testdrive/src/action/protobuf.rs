// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::iter;
use std::path::{self, Path};

use async_trait::async_trait;
use include_dir::{include_dir, Dir};
use itertools::Itertools;
use protobuf_native::compiler::{
    DiskSourceTree, SimpleErrorCollector, SourceTreeDescriptorDatabase,
};
use protobuf_native::MessageLite;
use tokio::fs;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

const WELL_KNOWN_PROTOS: Dir = include_dir!("$DEP_PROTOBUF_SRC_ROOT/include/google/protobuf");

pub struct CompileDescriptorsAction {
    inputs: Vec<String>,
    output: String,
}

pub fn build_compile_descriptors(
    mut cmd: BuiltinCommand,
) -> Result<CompileDescriptorsAction, String> {
    let inputs: Vec<String> = cmd
        .args
        .string("inputs")?
        .split(',')
        .map(|s| s.into())
        .collect();
    let output = cmd.args.string("output")?;
    for path in inputs.iter().chain(iter::once(&output)) {
        if path.contains(path::MAIN_SEPARATOR) {
            // The goal isn't security, but preventing mistakes.
            return Err("separators in paths are forbidden".into());
        }
    }
    Ok(CompileDescriptorsAction { inputs, output })
}

#[async_trait]
impl Action for CompileDescriptorsAction {
    async fn undo(&self, _: &mut State) -> Result<(), String> {
        // Files are written to a fresh temporary directory, so no need to
        // explicitly remove the file here.
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let out = {
            let wkt_dir = tempfile::tempdir()
                .map_err(|e| format!("creating well-known protos temp dir: {}", e))?;
            for file in WELL_KNOWN_PROTOS.files() {
                fs::write(wkt_dir.path().join(file.path()), file.contents())
                    .await
                    .map_err(|e| format!("writing well-known proto: {}", e))?;
            }

            let mut source_tree = DiskSourceTree::new();
            source_tree
                .as_mut()
                .map_path(Path::new(""), &state.temp_path);
            source_tree
                .as_mut()
                .map_path(Path::new("google/protobuf"), wkt_dir.path());

            let mut error_collector = SimpleErrorCollector::new();
            let mut db = SourceTreeDescriptorDatabase::new(source_tree.as_mut());
            db.as_mut().record_errors_to(error_collector.as_mut());
            let mut fds = match db.as_mut().build_file_descriptor_set(&self.inputs) {
                Ok(fds) => fds,
                Err(_) => {
                    drop(db);
                    return Err(format!(
                        "compiling protobuf descriptors:\n{}",
                        error_collector
                            .as_mut()
                            .into_iter()
                            .map(|e| e.to_string())
                            .join("\n")
                    ));
                }
            };
            fds.as_mut()
                .serialize()
                .map_err(|_| format!("failed serializing protobuf descriptors"))?
        };

        fs::write(state.temp_path.join(&self.output), out)
            .await
            .map_err(|e| format!("writing protobuf descriptors: {}", e))?;

        Ok(())
    }
}
