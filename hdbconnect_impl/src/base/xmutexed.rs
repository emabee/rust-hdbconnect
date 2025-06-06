#[derive(Debug)]
pub(crate) enum XMutexed<T> {
    #[cfg(feature = "sync")]
    Sync(std::sync::Mutex<T>),
    #[cfg(feature = "async")]
    Async(tokio::sync::Mutex<T>),
    #[cfg(not(any(feature = "sync", feature = "async")))]
    Dummy(T),
}
impl<T> XMutexed<T> {
    #[cfg(feature = "sync")]
    pub(crate) fn new_sync(inner: T) -> Self {
        Self::Sync(std::sync::Mutex::new(inner))
    }

    #[cfg(feature = "async")]
    pub(crate) fn new_async(inner: T) -> Self {
        Self::Async(tokio::sync::Mutex::new(inner))
    }

    #[cfg(feature = "sync")]
    pub(crate) fn lock_sync(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, T>, std::sync::PoisonError<std::sync::MutexGuard<'_, T>>>
    {
        match self {
            #[cfg(feature = "sync")]
            Self::Sync(m) => m.lock(),
            #[cfg(feature = "async")]
            Self::Async(_) => unimplemented!("asdad"),
            #[cfg(not(any(feature = "sync", feature = "async")))]
            Self::Dummy(_) => unimplemented!("dummy"),
        }
    }

    #[cfg(feature = "async")]
    pub(crate) async fn lock_async(&self) -> tokio::sync::MutexGuard<'_, T> {
        match self {
            #[cfg(feature = "sync")]
            Self::Sync(_) => unimplemented!("ertetr"),
            #[cfg(feature = "async")]
            Self::Async(m) => m.lock().await,
        }
    }
}

pub(crate) type AM<T> = std::sync::Arc<XMutexed<T>>;
#[allow(clippy::upper_case_acronyms)]
pub(crate) type OAM<T> = Option<AM<T>>;

#[cfg(feature = "sync")]
pub(crate) fn new_am_sync<T>(t: T) -> AM<T> {
    std::sync::Arc::new(XMutexed::new_sync(t))
}

#[cfg(feature = "async")]
pub(crate) fn new_am_async<T>(t: T) -> AM<T> {
    std::sync::Arc::new(XMutexed::new_async(t))
}
