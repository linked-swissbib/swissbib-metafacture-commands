# swissbib-metafacture-commands
Plugin with additional Metafacture commands used in linked-swissbib workflows

## List of commands

The commands are divided into several categories:
* Readers:
    * [read-json](#read-json): Parses JSON files
* Decoders:
    * [decode-ntriples](#decode-ntriples): Parses Ntriples files
    * [handle-marcxml-sb](#handle-marcxml-sb): Directly transforms MARC-XML fields to CSV rows like record-id,field,indicator1,indicator2,subfield,value
    * [handle-marcxml-sru](#handle-marcxml-sru): Handles MARC-XML files received from the SRU interface of Swissbib
* Pipe:
    * [encode-esbulk](#encode-esbulk): Encodes data as JSON-LD or in a special format suitable for bulk indexing in Elasticsearch
    * [encode-neo4j](#encode-neo4j): Encodes data as csv files suitable for batch uploads to a Neo4j database
    * [encode-ntriples](#encode-ntriples): Encodes data as Ntriples
    * [ext-filter](#ext-filter): Extends the default filter command in Flux by providing a parameter to implement a "filter not" mechanism
    * [itemerase-es](#itemerase-ex): Deletes items which belong to a certain bibliographicResource
    * [lookup-es](#lookup-es): Filters out records whose identifier already exists in an Elasticsearch index
    * [split-entities](#split-entities): Splits entities into individual records.
    * [update-es-id](#update-es-id): Identifies partially modified documents by comparing them to an Elasticsearch index.
* Writers:
    * [index-esbulk](#index-esbulk): Uses the bulk mechanisms of Elasticsearch to index records
    * [index-neo4j](#index-neo4j): Indexes nodes and relationships in Neo4j
    * [write-csv](#write-csv): Serialises data as CSV file with optional header.
    * [write-esbulk](#write-esbulk): Writes records as JSON files which can comply with the requirements of the Bulk API of Elasticsearch.
    * [write-kafka](#write-kafka): Acts as a producer in a Kafka cluster.
    * [write-neo4j](#write-neo4j): Writes csv files for batch uploading to a new Neo4j database.
    * [write-rdf-1line](#write-rdf-1line): Writes RDF-XML files, one line per record.
    * [write-socket](#write-socket): Sets up a socket server.
* Source:
    * [open-multi-http](#open-multi-http): Allows to open HTTP resources in a "paging" manner, e.g. to get data by chunks from a database
* Morph Functions:
    * [AuthorHash](#authorhash): Creates a hash value for authors based on different MARC fields.
    * [ItemHash](#itemhash): Creates a hash value for items based on different MARC fields.



### (#AuthorHash)

*Creates a hash value for authors based on different MARC fields.*

* Implementation: `org.swissbib.linked.mf.morph.functions.AuthorHash`

Resources:
* [Morph definition](https://github.com/linked-swissbib/mfWorkflows/blob/master/src/main/resources/transformation/indexWorkflows/morphModules/authorHash100.xml) for an author name in field 100
* [Morph definition](https://github.com/linked-swissbib/mfWorkflows/blob/master/src/main/resources/transformation/indexWorkflows/morphModules/authorHash700.xml) for an author name in field 700
* [Morph definition](https://github.com/linked-swissbib/mfWorkflows/blob/master/src/main/resources/transformation/indexWorkflows/morphModules/authorHash710.xml) for an organisation name in field 710
* [Morph definition](https://github.com/linked-swissbib/mfWorkflows/blob/master/src/main/resources/transformation/indexWorkflows/morphModules/authorHash711.xml) for an organisation name in field 711


### decode-ntriples

*Parses Ntriples-decoded records.*

* Implementation: `org.swissbib.linked.mf.decoder.NtriplesDecoder`
* In: `java.io.Reader`
* Out: `org.culturegraph.mf.framework.StreamReceiver`
* Option: `unicodeEscapeSeq`: "true", "false"

TODO: Example

### encode-esbulk

*Encodes records for bulk uploading to Elasticsearch.*

* Implementation: `org.swissbib.linked.mf.pipe.ESBulkEncoder`
* In: `org.culturegraph.mf.framework.StreamReceiver`
* Out: `java.lang.String`
* Options:
    * avoidMergers: If set to true, fields with same keys are modelled as separate inner objects instead of having their values merged (Boolean; default: false)
    * header: Should header for ES bulk be written (Boolean; default: true)? Warning: Setting this parameter to false will result in an invalid Bulk format!
    * escapeChars: Escapes prohibited characters in JSON strings (Boolean; default: true)
    * index: Index name of records
    * type: Type name of records

TODO: Example

### encode-neo4j

*Encodes records as csv files for batch uploading them to a new Neo4j-database. As the headers of the csv files are hardcoded, it is not ready to be used in a broader context.*

* Implementation: `org.swissbib.linked.mf.pipe.NeoEncoder`
* In: `org.culturegraph.mf.framework.StreamReceiver`
* Out: `java.lang.String`

[Example](https://github.com/guenterh/gndHackathon2016/blob/master/examples/gh/hackathonGND/gnd.flux)


### encode-ntriples

*Encodes data as Ntriples*

* Implementation: `org.swissbib.linked.mf.pipe.NtriplesEncoder`
* In: `org.culturegraph.mf.framework.StreamReceiver`
* Out: `java.lang.String`

TODO: Example


### ext-filter

*Extends the default filter command in Flux by providing a parameter to implement a "filter not" mechanism*

* Implementation: `org.swissbib.linked.mf.pipe.ExtFilter`
* In: `org.culturegraph.mf.framework.StreamReceiver`
* Out: `org.culturegraph.mf.framework.StreamReceiver`
* Option: `filterNot`: If set to true, filters out all records which match (Boolean; default: false)

TODO: Example


### handle-marcxml-sb

*Directly transforms MARC-XML fields to CSV rows like record-id,field,indicator1,indicator2,subfield,value*

* Implementation: `org.swissbib.linked.mf.decoder.MarcXmlSbHandler`
* In: `org.culturegraph.mf.framework.XmlReceiver`
* Out: `java.lang.String`

TODO: Example


### handle-marcxml-sru

*Handles MARC-XML files received from the SRU interface of Swissbib*

* Implementation: `org.swissbib.linked.mf.decoder.MarcXmlSruHandler`
* In: `org.culturegraph.mf.framework.XmlReceiver`:
* Out: `org.culturegraph.mf.framework.StreamReceiver`

TODO: Example


### index-esbulk

*Indexes records in Elasticsearch.*

* Implementation: `org.swissbib.linked.mf.pipe.ESBulkIndexer`
* In: `java.lang.Object`
* Out: `java.lang.Void`
* Options:
    * esClustername: Elasticsearch cluster name
    * recordsPerUpload: Number of records per single bulk upload
    * esNodes: Elasticsearch nodes. Nodes are separated by #

TODO: Example


### index-neo4j

*Indexes fields in Neo4j. Because the selection of the fields which are to be indexed is hardcoded, the benefit of this command is somewhat limited.*

* Implementation: `org.swissbib.linked.mf.writer.NeoIndexer`
* In: `org.culturegraph.mf.framework.StreamReceiver`
* Out: `org.culturegraph.mf.framework.StreamReceiver`
* Options:
    * batchSize: Size of batch upload for Neo4j
    * dbDir: Path to Neo4j database

TODO: Example


### ItemHash

*Creates a hash value for items based on different MARC fields.*

* Implementation: `org.swissbib.linked.mf.morph.functions.ItemHash`

Resource: [Morph definition](https://github.com/linked-swissbib/mfWorkflows/blob/master/src/main/resources/transformation/indexWorkflows/itemMorph.xml) which uses the item hash generator


### itemerase-es

*Deletes items which belong to a certain bibliographicResource. Recommended for internal use only.*

* Implementation: `org.swissbib.linked.mf.pipe.ESItemErase`
* In: `org.culturegraph.mf.framework.StreamReceiver`
* Out: `org.culturegraph.mf.framework.StreamReceiver`
* Options:
    * esClustername: Elasticsearch cluster name
    * esNodes: Elasticsearch nodes. Nodes are separated by #
    * esIndex: Elasticsearch index
    * esType: Document type for Elasticsearch

TODO: Example


### lookup-es

*Filters out records whose identifier already exists in an Elasticsearch index.*

* Implementation: `org.swissbib.linked.mf.pipe.ESLookup`
* In: `org.culturegraph.mf.framework.StreamReceiver`
* Out: `org.culturegraph.mf.framework.StreamReceiver`
* Options:
    * esClustername: Elasticsearch cluster name
    * esNodes: Elasticsearch nodes. Nodes are separated by #
    * esIndex: Elasticsearch index
    * esType: Document type for Elasticsearch

TODO: Example


### open-multi-http

*Allows to open HTTP resources in a "paging" manner, e.g. to get data by chunks from a database. You have to define two variable parts in the URL: `${cs}`, which sets the chunk size, and `${pa}`, which sets the offset.*

* Implementation: `org.swissbib.linked.mf.source.MultiHttpOpener`
* In: `java.lang.String`
* Out: `java.lang.Reader`
* Options:
    * accept: The accept header in the form type/subtype, e.g. text/plain.
    * encoding: The encoding is used to encode the output and is passed as Accept-Charset to the http connection.
    * lowerBound: Initial offset
    * upperBound: Limit
    * chunkSize: Number of documents to be downloaded in a single retrieval

TODO: Example


### read-json

*Parses JSON files*

* Implementation: `org.swissbib.linked.mf.reader.JsonReader`
* In: `java.lang.String`
* Out: `org.culturegraph.mf.framework.StreamReceiver`
* Option: `nullValues`: By default, null values are returned as empty strings. With nullValues this behaviour can be changed.

TODO: Example


### split-entitites

*Splits entities into individual records.*

* Implementation: `org.swissbib.linked.mf.pipe.EntitySplitter`
* In: `org.culturegraph.mf.framework.StreamReceiver`
* Out: `org.culturegraph.mf.framework.StreamReceiver`
* Option: `entityBoundary`: Node depth for entity splitting

TODO: Example


### update-es-id

*Identifies partially modified documents by comparing them to an Elasticsearch index. Is tailored to the so-called baseline workflow of linked-swissbib, so it's probably useless for other purposes*

* Implementation: `org.swissbib.linked.mf.pipe.ESIdUpdate`
* In: `org.culturegraph.mf.framework.StreamReceiver`
* Out: `org.culturegraph.mf.framework.StreamReceiver`
* Options:
    * esClustername: Elasticsearch cluster name
    * esNodes: Elasticsearch nodes. Nodes are separated by #
    * esIndex: Elasticsearch index
    * esType: Document type for Elasticsearch
    * matchingFields: Fields which should be matched. # is delimiter.
    * sThreshold: Matching threshold
    * refPath: Name of person / organisation field in bibliographicResoruce
    * uriPrefix: Prefix for identifier (e.g. http://data.swissbib.ch/person/)
    * graphDbDir: Path to Neo4j database

TODO: Example

### write-csv

*Serialises data as CSV file with optional header*

* Implementation: `org.swissbib.linked.mf.writer.ContinuousCsvWriter`
* In: `java.lang.String`
* Out: `java.lang.Void`
* Options:
    * compression: Sets the compression mode
    * continuousFile: Boolean. If set to true, the header is only written to the first file.
    * encoding: Sets the encoding used by the underlying writer
    * filenamePostfix: By default the filename consists of a zero-filled sequential number with six digits. Sets a postfix for this number.
    * filenamePrefix: By default the filename consists of a zero-filled sequential number with six digits. Sets a prefix for this number.
    * filetype: File ending
    * footer: Sets the footer which is output after the last object
    * header: Sets the header which is output before the first object
    * linesPerFile: Number of lines written to one file
    * path: Path to directory with CSV files
    * separator: Sets the separator which is output between objects

TODO: Example


### write-esbulk

*Writes records as JSON files which comply with the requirements of the Bulk API of Elasticsearch.*

* Implementation: `org.swissbib.linked.mf.writer.ESBulkWriter`
* In: `java.lang.Object`
* Out: `java.lang.Void`
* Options:
    * compress: Should files be .gz-compressed? (Default is true)
    * filePrefix: Prefix for file names
    * fileSize: Number of records in one file
    * jsonCompliant: Should files be JSON compliant (Boolean; default: false)? Warning: Setting this parameter to true will result in an invalid Bulk format!
    * outDir: Root directory for output
    * subdirSize: Number of files in one subdirectory (Default: 300)
    * type: Type name of records (will only be attached to filename)

TODO: Example


### write-kafka

*Acts as a producer in a Kafka cluster.*

* Implementation: `org.swissbib.linked.mf.writer.KafkaWriter`
* In: `java.lang.Object`
* Out: `java.lang.Void`
* Option:
    * host: Hostname of Kafka cluster
    * port: Port of Kafka cluster

TODO: Example


### write-neo4j

*Writes csv files for batch uploading to a new Neo4j database. Intended to be used in junction with index-neo4j.*

* Implementation: `org.swissbib.linked.mf.writer.NeoWriter`
* In: `java.lang.Object`
* Out: `java.lang.Void`
* Options:
    * csvDir: Path to the output directory
    * csvFileLength: Numbers of records in one dedicated CSV file
    * batchWriteSize: Maximal number of records of the same category

[Example](https://github.com/guenterh/gndHackathon2016/blob/master/examples/gh/hackathonGND/gnd.flux)


### write-rdf-1line

*Writes RDF-XML files, one line per record.*

* Implementation: `org.swissbib.linked.mf.writer.SingleLineWriterRDFXml`
* In: `java.lang.Object`
* Out: `java.lang.Void`
* Options:
    * usecontributor: "true", "false"
    * rootTag: XML root tag
    * extension: File extension for output files
    * compress: Should output files be compressed? ("true", "false")
    * baseOutDir: Base directory for output files:
    * outFilePrefix: Prefix for output files
    * fileSize: Number of records in one file
    * subDirSize: Number of records in one subdirectory
    * type: Concept / type name

TODO: Example


### write-socket

*Sets up a socket server*

* Implementation: `org.swissbib.linked.mf.writer.SocketWriter`
* In: `java.lang.Object`
* Out: `java.lang.Void`
* Option: `port`: Port of socket server (host is always 127.0.0.1)

TODO: Example
