use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

const MONITOR_OPTION: &str = "dtpt_monitor";

pub fn tracking_allocator_static(_input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    quote! {
        #[cfg(feature = #MONITOR_OPTION)]
        #[global_allocator]
        static GLOBAL: tracking_allocator::Allocator<std::alloc::System> = tracking_allocator::Allocator::system();

        #[cfg(feature = #MONITOR_OPTION)]
        mod dtpt_monitor {
            use std::sync::Mutex;

            use tracking_allocator::{AllocationGroupId, AllocationTracker};

            pub struct MemoryTracker;

            pub struct MemoryTrackerData {
                pub object: usize,
                pub wrapped: usize,
            }

            pub static DATA: Mutex<MemoryTrackerData> = Mutex::new(MemoryTrackerData {
                object: 0,
                wrapped: 0,
            });

            impl AllocationTracker for MemoryTracker {
                fn allocated(
                    &self,
                    _addr: usize,
                    object_size: usize,
                    wrapped_size: usize,
                    _group_id: AllocationGroupId,
                    ) {
                    let mut data = DATA.lock().unwrap();
                    data.object += object_size;
                    data.wrapped += wrapped_size;
                }

                fn deallocated(
                    &self,
                    _addr: usize,
                    object_size: usize,
                    wrapped_size: usize,
                    _source_group_id: AllocationGroupId,
                    _current_group_id: AllocationGroupId,
                    ) {
                    let mut data = DATA.lock().unwrap();
                    data.object -= object_size;
                    data.wrapped -= wrapped_size;
                }
            }
        }
    }
    .into()
}

pub fn tracking_allocator_main(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let ItemFn {
        attrs,
        vis,
        sig,
        block,
    } = parse_macro_input!(item as ItemFn);

    quote! {
        #[cfg(not(feature = #MONITOR_OPTION))]
        #(#attrs)*
        #vis #sig {
            let ret = #block;

            ret
        }

        #[cfg(feature = #MONITOR_OPTION)]
        #(#attrs)*
        #vis #sig {
            use tracking_allocator::{AllocationGroupToken, AllocationRegistry, Allocator};

            AllocationRegistry::set_global_tracker(dtpt_monitor::MemoryTracker)
                .expect("no other global tracker should be set yet");

            let mut local_token =
                AllocationGroupToken::register().expect("failed to register allocation group");

            let local_guard = local_token.enter();

            AllocationRegistry::enable_tracking();

            let ret = #block;

            AllocationRegistry::disable_tracking();

            ret
        }
    }
    .into()
}
