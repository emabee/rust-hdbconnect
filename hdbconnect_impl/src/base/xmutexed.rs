pub(crate) enum XMutexed<T> {
    #[cfg(feature = "sync")]
    Sync(std::sync::Mutex<T>),
    #[cfg(feature = "async")]
    Async(tokio::sync::Mutex<T>),
}
impl<T> XMutexed<T> {
    #[cfg(feature = "sync")]
    fn lock_sync(
        &mut self,
    ) -> Result<std::sync::MutexGuard<'_, T>, std::sync::PoisonError<std::sync::MutexGuard<'_, T>>>
    {
        match self {
            Self::Sync(ref mut m) => m.lock(),
            Self::Async(_) => unimplemented!("asdad"),
        }
    }

    #[cfg(feature = "async")]
    async fn lock_async(&mut self) -> tokio::sync::MutexGuard<'_, T> {
        match self {
            Self::Sync(_) => unimplemented!("ertetr"),
            Self::Async(ref mut m) => m.lock().await,
        }
    }
}
