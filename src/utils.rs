pub trait ResultLogError {
    fn log_error(self) -> Self;
}

impl<T, E: std::fmt::Debug> ResultLogError for Result<T, E> {
    fn log_error(self) -> Self {
        if let Err(ref e) = self {
            log::error!("Error: {:?}", e);
        }
        self
    }
}