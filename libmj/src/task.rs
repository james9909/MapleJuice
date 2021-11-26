use crate::request::{MasterJuiceRequest, MasterMapleRequest};

/// Task encompasses all the types of tasks that we can support.
#[derive(Clone, Debug)]
pub enum Task {
    Maple(MasterMapleRequest),
    Juice(MasterJuiceRequest),
}
