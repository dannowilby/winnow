use wasmtime::component::HasData;
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxView, WasiView};

use crate::wasm::mapper::mapreduce::typeimpls::logging::Host;

pub struct HostAPI {
    pub wasi_ctx: WasiCtx,
    pub resource_table: ResourceTable,
}

impl Host for HostAPI {
    fn log(&mut self, msg: String) {
        println!("[Guest] {}", msg);
    }
}

impl WasiView for HostAPI {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi_ctx,
            table: &mut self.resource_table,
        }
    }
}

/// For unknown reasons, this allows the host api functions, and by extension
/// the whole project, compile.
pub struct ExtensionData;

impl HasData for ExtensionData {
    type Data<'a> = &'a mut HostAPI;
}
