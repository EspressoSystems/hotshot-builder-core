use hotshot_task::{
    boxed_sync,
    event_stream::ChannelStream,
    task::{FilterEvent, HandleEvent, HandleMessage, HotShotTaskCompleted, HotShotTaskTypes},
    task_impls::TaskBuilder,
    task_launcher::TaskRunner,
    GeneratedStream, Merge,
};

// process the hotshot transaction event 
pub async fn process_hotshot_transaction<Types: NodeType, I: NodeImplementation<TYPES>>(
    task_runner: TaskRunner,
    event_stream: ChannelStream<HotShotEvent<Types>>,
    handle: SystemContextHandle<Types, I>,
) -> TaskRunner
{
    let transactions_event_handler = HandleEvent(Arc::new(
        move |event, mut state: TransactionTaskState<TYPES, I, HotShotConsensusApi<TYPES, I>>| {
            async move {
                let completion_status = state.handle_event(event).await;
                (completion_status, state)
            }
            .boxed()
        },
    ));
}