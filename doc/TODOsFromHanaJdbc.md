# TODOs um hdbconnect funktional zu erweitern

## Authentication

Studiere

    hanajdbc::src/main/java/com/sap/db/util/security/AuthenticationManager.java:41 =

    public Session authenticate( SessionFactory factory, ConnectionSapDB connection, Session session,
                                    boolean isAnchorSession, String userName, String passwd, String x509,
                                    Set<AuthenticationMethodType> activeMethods,
                                    boolean isReattach,
                                    SessionReattachStatusOption[] outServerReattachStatus ) 
                                    throws RTEException, SQLException

und leite TODOs für hdbconnect ab.

## Compression

todo

## Analysiere und vergleiche was wir bisher haben zum Thema reconnect versus reattach

sesion cookie

todo

## redirect

todo

## anchor session

todo
