# References and Lifetimes within `hdbconnect` and `hdbconnect_async`

```mermaid

    flowchart

        Connection
        ConnectionCore>ConnectionCore]

        PreparedStatement

        ResultSet
        ResultSetMetadata
        RsState[RsState]
        Row
        Lob[B/C/NC-Lob]
        RsCore>RsCore]

        LobHandle[B/C/NC-LobHandle]

            ParameterDescriptors
            ParameterRows
            PreparedStatementCore>PreparedStatementCore]

    PreparedStatement  -. produces .->  ResultSet
    PreparedStatementCore -- ArcXMtx --> ConnectionCore
    Connection -- ArcXMtx --> ConnectionCore
    ConnectionCore -. produces .->  PreparedStatement
    ConnectionCore -. produces .->  ResultSet
    ResultSet -- ArcXMtx --> RsState
    ResultSet -- Arc --> ResultSetMetadata
    PreparedStatement -- ArcXMtx --> PreparedStatementCore
    PreparedStatement -- optional Arc --> ResultSetMetadata
    PreparedStatement --> ParameterDescriptors
    PreparedStatement -- batch--> ParameterRows
    Row -- Arc --> ResultSetMetadata
    RsState -- holds/loads --> Row
    RsState -- optional ArcXMtx --> RsCore
    RsCore -- ArcXMtx --> ConnectionCore
    RsCore -- optional ArcXMtx --> PreparedStatementCore
    Row -- value-iterator --> Lob
    Lob -- holds --> LobHandle
    LobHandle -- ArcXMtx --> ConnectionCore
    LobHandle -- optional ArcXMtx --> RsCore

classDef Public fill:#1af,stroke:#333,stroke-width:4px;
class Connection,ResultSet,Row,ResultSetMetadata,PreparedStatement,ParameterDescriptors,Lob Public;
```

Legend:

```mermaid
flowchart

    Public[Part of public API]
    Drop>Object with corresponding Server-side object]

    classDef Public fill:#1af,stroke:#333,stroke-width:4px;
    class Public Public;
```

`ArcXMtx` is either an `Arc<std::sync::Mutex>` or an `Arc<tokio::sync::Mutex>.`

## Sharing objects

Example: a `ResultSetMetadata` object e.g. can be used by a `ResultSet`, its `Row`s and a `PreparedStatement`.

## Rust Lifetimes control drop of server-side resources

The lifetimes of the public objects are controlled by the application.

By using the depicted Core objects and the internal references to them,
we ensure that each public object remains usable for its entire own lifetime,
without forcing the application to keep other objects alive.

The Core objects have a Drop implementation that releases the corresponding server-side ressource
when they are dropped themselves.

A `ResultSet` object e.g. will be able to fetch outstanding rows from the server
even if the application already dropped the conection object,
because it keeps the `RsCore` and the `ConnectionCore` objects alive until all data
are loaded.
