use serde;
use std::fmt;
use std::marker::Sized;

use super::RsDeserializer;
use super::deser_row::DeserializableRow;
use super::deserialization_error::DeserError;


pub trait DeserializableResultSet: fmt::Debug + Sized {
    type E: From<DeserError> + Sized;
    type ROW: DeserializableRow; // row

    /// Returns true if no rows are contained
    fn is_empty(&self) -> bool;

    /// Returns true if more than 1 row is contained
    fn has_multiple_rows(&self) -> bool;

    /// Returns the number of contained rows
    fn len(&mut self) -> Result<usize, DeserError>;

    /// Returns a pointer to the last row
    fn last_row(&self) -> Option<&Self::ROW>;

    /// Returns a mutable pointer to the last row
    fn last_row_mut(&mut self) -> Option<&mut Self::ROW>;

    /// Reverses the order of the rows
    fn reverse_rows(&mut self);

    /// Removes the last row and returns it, or None if it is empty.
    fn pop_row(&mut self) -> Option<Self::ROW>;

    /// Returns the number of fields in each row
    fn number_of_fields(&self) -> usize;

    /// Returns the name of the column at the specified index
    fn get_fieldname(&self, field_idx: usize) -> Option<&String>;

    /// Returns the number of result rows.
    fn no_of_rows(&self) -> usize;

    /// Fetches all not yet transported result lines from the server.
    ///
    /// Bigger resultsets are typically not transported in one DB roundtrip;
    /// the number of roundtrips depends on the size of the resultset
    /// and the configured fetch_size of the connection.
    fn fetch_all(&mut self) -> Result<(), Self::E>;

    /// Translates a generic resultset into a given rust type (that implements Deserialize).
    ///
    /// A resultset is essentially a two-dimensional structure, given as a list of rows
    /// (a <code>Vec&lt;Row&gt;</code>),
    /// where each row is a list of fields (a <code>Vec&lt;TypedValue&gt;</code>);
    /// the name of each field is given in the metadata of the resultset.
    ///
    /// The method supports a variety of target data structures, with the only strong limitation
    /// that no data loss is supported.
    ///
    /// * It depends on the dimension of the resultset what target data structure
    ///   you can choose for deserialization:
    ///
    ///     * You can always use a <code>Vec&lt;line_struct&gt;</code>, where
    ///       <code>line_struct</code> matches the field list of the resultset.
    ///
    ///     * If the resultset contains only a single line (e.g. because you specified
    ///       TOP 1 in your select),
    ///       then you can optionally choose to deserialize into a plain <code>line_struct</code>.
    ///
    ///     * If the resultset contains only a single column, then you can optionally choose to
    ///       deserialize into a <code>Vec&lt;plain_field&gt;</code>.
    ///
    ///     * If the resultset contains only a single value (one row with one column),
    ///       then you can optionally choose to deserialize into a plain <code>line_struct</code>,
    ///       or a <code>Vec&lt;plain_field&gt;</code>, or a plain variable.
    ///
    /// * Also the translation of the individual field values provides a lot of flexibility.
    ///   You can e.g. convert values from a nullable column into a plain field,
    ///   provided that no NULL values are given in the resultset.
    ///
    ///   Vice versa, you always can use an Option<code>&lt;plain_field&gt;</code>,
    ///   even if the column is marked as NOT NULL.
    ///
    /// * Similarly, integer types can differ, as long as the concrete values can
    ///   be assigned without loss.
    ///
    /// Note that you need to specify the type of your target variable explicitly, so that
    /// <code>into_typed()</code> can derive the type it needs to serialize into:
    ///
    /// ```ignore
    /// #[derive(Deserialize)]
    /// struct MyStruct {
    ///     ...
    /// }
    /// let typed_result: Vec<MyStruct> = resultset.into_typed()?;
    /// ```
    fn into_typed<'de, T>(mut self) -> Result<T, Self::E>
        where T: serde::de::Deserialize<'de>,
              Self: Sized
    {
        trace!("DeserializableResultSet::into_typed()");
        self.fetch_all()?;
        Ok(serde::de::Deserialize::deserialize(&mut RsDeserializer::new(self))?)
    }
}
