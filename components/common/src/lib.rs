
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Temp(pub u32);