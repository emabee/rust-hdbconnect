pub trait IntoString {
    fn into(self) -> String;
}

impl IntoString for &String {
    fn into(self) -> String {
        self.to_string()
    }
}
impl IntoString for &str {
    fn into(self) -> String {
        self.to_string()
    }
}

impl IntoString for String {
    fn into(self) -> String {
        self
    }
}
