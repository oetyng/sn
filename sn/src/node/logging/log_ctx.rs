// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use xor_name::Prefix;

#[allow(unused)]
pub(crate) struct LogCtx {}

impl LogCtx {
    #[allow(unused)]
    pub(crate) async fn prefix(&self) -> Prefix {
        unimplemented!()
    }
}

// use futures::Future;
// use xor_name::Prefix;

// pub(crate) struct LogCtx<F, Fut>
// where
//     F: Fn() -> Fut,
//     Fut: Future<Output = Prefix>,
// {
//     prefix_src: F,
// }

// impl<F, Fut> LogCtx<F, Fut> {
//     pub(crate) fn new(prefix_src: F) -> Self {
//         Self { prefix_src }
//     }

//     pub(crate) async fn prefix(&self) -> Prefix {
//         self.prefix_src.await
//     }
// }
