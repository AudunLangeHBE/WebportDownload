# WebportDownload
WebportDownload er et program som laster ned data fra Webport installasjon til en SQL Server instans. 
Informasjon om Undersøkelser, spørsmål, svaralternativer og svar blir hentet ut fra Kilde (webport SQL Server) og lagret i mål (SQL Server).
Etter nedlasting, blir nye forekomster av undersøkelser/spørsmål/svaralternativ lagt inn i endelige tabeller. Svar på undersøkelsen blir alltid lastet ned bare en gang. Webport tilgjengeliggjør disse i et «CreateSet» for besvarelser. Dette blir så lastet ned, før kvittering blir returnert til Webport om vellykket nedlasting. Etter en slik vellykket nedlasting, så har Webport registrert besvarelsene som ferdig kvittert ut, og de vil ikke være tilgjengelig for nedlasting neste gang.
Prosedyrer for oppdatering av data etter nedlasting, er definert i mål-server, og kalles opp fra WebportDownload etter dataoverføring er utført.
Disse må være på plass i måldatabase, før programmet kan kjøres.
WebportDownload er utviklet i C#/.NET Target Framework 4.5, som en konsoll-applikasjon.

Konfigurasjon
Det er tre innstillinger som kan settes i konfigurasjon (WebportDownload.exe.config):
-	sourceConnection (kilde – Webport SQL Server)
-	destConnection (Mål – SQL Server)
-	CompanyID (Tekst-variabel som er unik for installasjon av Webport)
Disse må spesifiseres.

Forberedelse av målserver
Målserver, som skal være av type SQL Server må ha de tilhørende tabeller og prosedyrer.
Når Database er opprettet, kan tabeller og prosedyrer settes opp med medfølgende script.
WebportDownload_Målserver_DDL.SQL
