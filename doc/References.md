# Test

```mermaid

    flowchart

    subgraph All[Main objects in hdbconnect and how they reference each other]
        direction TB

        Public
        Intern
    end

    subgraph Public
        direction TB

        Connection
        ResultSet
        ResultSetMetadata
        Row
        PreparedStatement
        Lob[B/C/NC-Lob]
    end

    subgraph Intern
        direction TB
        RsState[RsState\nServerUsage\nRows]
        PsCore>PsCore, Drop]
        ConnectionCore>ConnectionCore, Drop]
        RsCore>RsCore, Drop]
        LobHandle[B/C/NC-LobHandle]
        ParameterDescriptors
        ParameterRows
    end

    PsCore -- ArcMutex --> ConnectionCore
    Connection -- ArcMutex --> ConnectionCore
    ConnectionCore -. produces .->  PreparedStatement
    ConnectionCore -. produces .->  ResultSet
    ResultSet -- ArcMutex --> RsState
    ResultSet -- Arc --> ResultSetMetadata
    PreparedStatement -- ArcMutex --> PsCore
    PreparedStatement -- optional ArcMutex --> ResultSetMetadata
    PreparedStatement --> ParameterDescriptors
    PreparedStatement --> ParameterRows
    Row -- Arc --> ResultSetMetadata
    RsState -- holds/loads --> Row
    RsState -- optional ArcMutex --> RsCore
    RsCore -- ArcMutex --> ConnectionCore
    RsCore -- optional ArcMutex --> PsCore
    Row -- value-iterator --> Lob
    Lob -- holds --> LobHandle
    LobHandle -- ArcMutex --> ConnectionCore
    LobHandle -- optional ArcMutex --> RsCore
```

Objects with Drop implementation (ConnectionCore, ResultSetCore, PreparedStatementCore)
release the corresponding server-side ressource when they are dropped themselves.

The hard ref chain from LobHandle to ConnCore ia needed for `read_slice`,
which is supposed to work independently from the streaming-like content loading.

TODO: don't we then also need the RsCore, which is thrown away once the content was loaded completely?
