# dapaas-air-quality

## Usage

### CLI

#### Graft all data

Create graphs from each file in the FTP server

    $ lein run

The graphs are stored in `graphs/`

#### Graft the last n data

    $ lein run n

#### Example

    $ lein run 3
    Grafted:  864150_20150617_0600.csv
    Grafted:  864150_20150617_0700.csv
    Grafted:  864150_20150617_0800.csv

### REPL

    aqmesh-graft.core> (-main)
    Grafted:  688150_20150526_0000.csv
    Grafted:  688150_20150526_0100.csv
    Grafted:  688150_20150526_0200.csv
    ...


    aqmesh-graft.core> (-main 2)
    Grafted:  864150_20150617_0700.csv
    Grafted:  864150_20150617_0800.csv
    (true true)
