# swissbib-metafacture-commands
Plugin with additional Metafacture commands used in linked-swissbib workflows

## List of commands

The commands are divided into several categories:
* Decoders:
    * [decode-json](#decode-json): Parses JSON files
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
    * [read-kafka](#read-kafka): Acts as a Kafka Consumer for Metafacture
    * [open-multi-http](#open-multi-http): Allows to open HTTP resources in a "paging" manner, e.g. to get data by chunks from a database
* Record Splitters:
    * [read-json-object](#read-json-object): Reads in a JSON file and splits it at the end of the root object / array.
* Morph Functions:
    * [AuthorHash](#authorhash): Creates a hash value for authors based on different MARC fields.
    * [ItemHash](#itemhash): Creates a hash value for items based on different MARC fields.



### AuthorHash

*Creates a hash value for authors based on different MARC fields.*

* Implementation: [org.swissbib.linked.mf.morph.functions.AuthorHash](https://github.com/linked-swissbib/swissbib-metafacture-commands/blob/master/src/main/java/org/swissbib/linked/mf/morph/functions.AuthorHash.java)

Resources:
* [Morph definition](https://github.com/linked-swissbib/mfWorkflows/blob/master/src/main/resources/transformation/indexWorkflows/morphModules/authorHash100.xml) for an author name in field 100
* [Morph definition](https://github.com/linked-swissbib/mfWorkflows/blob/master/src/main/resources/transformation/indexWorkflows/morphModules/authorHash700.xml) for an author name in field 700
* [Morph definition](https://github.com/linked-swissbib/mfWorkflows/blob/master/src/main/resources/transformation/indexWorkflows/morphModules/authorHash710.xml) for an organisation name in field 710
* [Morph definition](https://github.com/linked-swissbib/mfWorkflows/blob/master/src/main/resources/transformation/indexWorkflows/morphModules/authorHash711.xml) for an organisation name in field 711


### decode-json

*Parses JSON. Preferably used in conjunction with [read-json-object](#read-json-object)*

* Implementation: [org.swissbib.linked.mf.decode.JsonDecoder](https://github.com/linked-swissbib/mfWorkflows/blob/master/src/main/java/org/swissbib/linked/mf/decoder/JsonDecoder.java)
* In: `java.io.Reader`
* Out: [org.culturegraph.mf.framework.StreamReceiver](https://github.com/culturegraph/metafacture-core/blob/master/src/main/java/org/culturegraph/mf/framework/StreamReceiver.java)
* Option: `nullValues`: Set if null values should not be returned as empty strings.

### decode-ntriples

*Parses Ntriples-encoded records.*

* Implementation: [org.swissbib.linked.mf.decoder.NtriplesDecoder](https://github.com/linked-swissbib/swissbib-metafacture-commands/blob/master/src/main/java/org/swissbib/linked/mf/decoder/NtriplesDecoder.java)
* In: `java.io.Reader`
* Out: [org.culturegraph.mf.framework.StreamReceiver](https://github.com/culturegraph/metafacture-core/blob/master/src/main/java/org/culturegraph/mf/framework/StreamReceiver.java)
* Option: `unicodeEscapeSeq`: "true", "false"

Example: [linked-swissbib "EnrichedLine"](https://github.com/sschuepbach/metafacture-examples/tree/master/Swissbib-Extensions/Linked-Swissbib-Enrichedline)

### encode-esbulk

*Encodes records for bulk uploading to Elasticsearch.*

* Implementation: [org.swissbib.linked.mf.pipe.ESBulkEncoder](https://github.com/linked-swissbib/swissbib-metafacture-commands/blob/master/src/main/java/org/swissbib/linked/mf/pipe/ESBulkEncoder.java)
* In: [org.culturegraph.mf.framework.StreamReceiver](https://github.com/culturegraph/metafacture-core/blob/master/src/main/java/org/culturegraph/mf/framework/StreamReceiver.java)
* Out: `java.lang.String`
* Options:
    * avoidMergers: If set to true, fields with same keys are modelled as separate inner objects instead of having their values merged (Boolean; default: false)
    * header: Should header for ES bulk be written (Boolean; default: true)? Warning: Setting this parameter to false will result in an invalid Bulk format!
    * escapeChars: Escapes prohibited characters in JSON strings (Boolean; default: true)
    * index: Index name of records
    * type: Type name of records

Example: [linked-swissbib "Baseline"](https://github.com/sschuepbach/metafacture-examples/tree/master/Swissbib-Extensions/Linked-Swissbib-Baseline)

### encode-neo4j

*Encodes records as csv files for batch uploading them to a new Neo4j-database. As the headers of the csv files are hardcoded, it is not ready to be used in a broader context.*

* Implementation: [org.swissbib.linked.mf.pipe.NeoEncoder](https://github.com/linked-swissbib/swissbib-metafacture-commands/blob/master/src/main/java/org/swissbib/linked/mf/pipe/NeoEncoder.java)
* In: [org.culturegraph.mf.framework.StreamReceiver](https://github.com/culturegraph/metafacture-core/blob/master/src/main/java/org/culturegraph/mf/framework/StreamReceiver.java)
* Out: `java.lang.String`

Example: [Graph visualisation of the GND](https://github.com/guenterh/gndHackathon2016/blob/master/examples/gh/hackathonGND)


### encode-ntriples

*Encodes data as Ntriples*

* Implementation: [org.swissbib.linked.mf.pipe.NtriplesEncoder](https://github.com/linked-swissbib/swissbib-metafacture-commands/blob/master/src/main/java/org/swissbib/linked/mf/pipe/NtriplesEncoder.java)
* In: [org.culturegraph.mf.framework.StreamReceiver](https://github.com/culturegraph/metafacture-core/blob/master/src/main/java/org/culturegraph/mf/framework/StreamReceiver.java)
* Out: `java.lang.String`

Example: [Libadmin entries as Ntriples](https://github.com/sschuepbach/metafacture-examples/blob/master/Swissbib-Extensions/Libadmin-Ntriples/libadminFlux.flux)


### ext-filter

*Extends the default filter command in Flux by providing a parameter to implement a "filter not" mechanism*

* Implementation: [org.swissbib.linked.mf.pipe.ExtFilter](https://github.com/linked-swissbib/swissbib-metafacture-commands/blob/master/src/main/java/org/swissbib/linked/mf/pipe/ExtFilter.java)
* In: [org.culturegraph.mf.framework.StreamReceiver](https://github.com/culturegraph/metafacture-core/blob/master/src/main/java/org/culturegraph/mf/framework/StreamReceiver.java)
* Out: [org.culturegraph.mf.framework.StreamReceiver](https://github.com/culturegraph/metafacture-core/blob/master/src/main/java/org/culturegraph/mf/framework/StreamReceiver.java)
* Option: `filterNot`: If set to true, returns only records which don't match a certain criteria (Boolean; default: false)

Example: [Show record ids which don't have a title (MARC field 245$a)](https://github.com/sschuepbach/metafacture-examples/tree/master/Swissbib-Extensions/Records-without-titles)


### handle-marcxml-sb

*Directly transforms MARC-XML fields to CSV rows like record-id,field,indicator1,indicator2,subfield,value*

* Implementation: [org.swissbib.linked.mf.decoder.MarcXmlSbHandler](https://github.com/linked-swissbib/swissbib-metafacture-commands/blob/master/src/main/java/org/swissbib/linked/mf/decoder/MarcXmlSbHandler.java)
* In: [org.culturegraph.mf.framework.XmlReceiver](https://github.com/culturegraph/metafacture-core/blob/master/src/main/java/org/culturegraph/mf/framework/XmlReceiver.java)
* Out: `java.lang.String`

Example: [1:1 transformation of MARC-XML to CSV](https://github.com/sschuepbach/metafacture-examples/tree/master/Swissbib-Extensions/MARC-CSV)


### handle-marcxml-sru

*Handles MARC-XML files received from the SRU interface of Swissbib*

* Implementation: [org.swissbib.linked.mf.decoder.MarcXmlSruHandler](https://github.com/linked-swissbib/swissbib-metafacture-commands/blob/master/src/main/java/org/swissbib/linked/mf/decoder/MarcXmlSruHandler.java)
* In: [org.culturegraph.mf.framework.XmlReceiver](https://github.com/culturegraph/metafacture-core/blob/master/src/main/java/org/culturegraph/mf/framework/XmlReceiver.java)
* Out: [org.culturegraph.mf.framework.StreamReceiver](https://github.com/culturegraph/metafacture-core/blob/master/src/main/java/org/culturegraph/mf/framework/StreamReceiver.java)

Example: [Workflow which queries the Swissbib SRU interface and filters, transforms and dumps the results to a CSV file](https://github.com/sschuepbach/metafacture-examples/tree/master/Swissbib-Extensions/Swissbib-SRU)


### index-esbulk

*Indexes records in Elasticsearch.*

* Implementation: [org.swissbib.linked.mf.pipe.ESBulkIndexer](https://github.com/linked-swissbib/swissbib-metafacture-commands/blob/master/src/main/java/org/swissbib/linked/mf/pipe/ESBulkIndexer.java)
* In: `java.lang.Object`
* Out: `java.lang.Void`
* Options:
    * esClustername: Elasticsearch cluster name
    * recordsPerUpload: Number of records per single bulk upload
    * esNodes: Elasticsearch nodes. Nodes are separated by #

Example: [linked-swissbib "Baseline"](https://github.com/sschuepbach/metafacture-examples/tree/master/Swissbib-Extensions/Linked-Swissbib-Baseline)


### index-neo4j

*Indexes fields in Neo4j. Because the selection of the fields which are to be indexed is hardcoded, the benefit of this command outside our admittedly narrow scope is somewhat limited.*

* Implementation: [org.swissbib.linked.mf.writer.NeoIndexer](https://github.com/linked-swissbib/swissbib-metafacture-commands/blob/master/src/main/java/org/swissbib/linked/mf/writer/NeoIndexer.java)
* In: [org.culturegraph.mf.framework.StreamReceiver](https://github.com/culturegraph/metafacture-core/blob/master/src/main/java/org/culturegraph/mf/framework/StreamReceiver.java)
* Out: [org.culturegraph.mf.framework.StreamReceiver](https://github.com/culturegraph/metafacture-core/blob/master/src/main/java/org/culturegraph/mf/framework/StreamReceiver.java)
* Options:
    * batchSize: Size of batch upload for Neo4j
    * dbDir: Path to Neo4j database


### ItemHash

*Creates a hash value for items based on different MARC fields.*

* Implementation: [org.swissbib.linked.mf.morph.functions.ItemHash](https://github.com/linked-swissbib/swissbib-metafacture-commands/blob/master/src/main/java/org/swissbib/linked/mf/morph/functions.ItemHash.java)

Resource: [Morph definition](https://github.com/linked-swissbib/mfWorkflows/blob/master/src/main/resources/transformation/indexWorkflows/itemMorph.xml) which uses the item hash generator


### itemerase-es

*Deletes items which belong to a certain bibliographicResource. Recommended for internal use only. Intended to use with the tracking framework of linked-swissbib*

* Implementation: [org.swissbib.linked.mf.pipe.ESItemErase](https://github.com/linked-swissbib/swissbib-metafacture-commands/blob/master/src/main/java/org/swissbib/linked/mf/pipe/ESItemErase.java)
* In: [org.culturegraph.mf.framework.StreamReceiver](https://github.com/culturegraph/metafacture-core/blob/master/src/main/java/org/culturegraph/mf/framework/StreamReceiver.java)
* Out: [org.culturegraph.mf.framework.StreamReceiver](https://github.com/culturegraph/metafacture-core/blob/master/src/main/java/org/culturegraph/mf/framework/StreamReceiver.java)
* Options:
    * esClustername: Elasticsearch cluster name
    * esNodes: Elasticsearch nodes. Nodes are separated by #
    * esIndex: Elasticsearch index
    * esType: Document type for Elasticsearch


### lookup-es

*Filters out records whose identifier already exists in an Elasticsearch index. Intended to use with the tracking framework of linked-swissbib.*

* Implementation: [org.swissbib.linked.mf.pipe.ESLookup](https://github.com/linked-swissbib/swissbib-metafacture-commands/blob/master/src/main/java/org/swissbib/linked/mf/pipe/ESLookup.java)
* In: [org.culturegraph.mf.framework.StreamReceiver](https://github.com/culturegraph/metafacture-core/blob/master/src/main/java/org/culturegraph/mf/framework/StreamReceiver.java)
* Out: [org.culturegraph.mf.framework.StreamReceiver](https://github.com/culturegraph/metafacture-core/blob/master/src/main/java/org/culturegraph/mf/framework/StreamReceiver.java)
* Options:
    * esClustername: Elasticsearch cluster name
    * esNodes: Elasticsearch nodes. Nodes are separated by #
    * esIndex: Elasticsearch index
    * esType: Document type for Elasticsearch


### open-multi-http

*Allows to open HTTP resources in a "paging" manner, e.g. to get data by chunks from a database. You have to define two variable parts in the URL: `${cs}`, which sets the chunk size, and `${pa}`, which sets the offset.*

* Implementation: [org.swissbib.linked.mf.source.MultiHttpOpener](https://github.com/linked-swissbib/swissbib-metafacture-commands/blob/master/src/main/java/org/swissbib/linked/mf/source/MultiHttpOpener.java)
* In: `java.lang.String`
* Out: `java.lang.Reader`
* Options:
    * accept: The accept header in the form type/subtype, e.g. text/plain.
    * encoding: The encoding is used to encode the output and is passed as Accept-Charset to the http connection.
    * lowerBound: Initial offset
    * upperBound: Limit
    * chunkSize: Number of documents to be downloaded in a single retrieval

Example: [Workflow which queries the Swissbib SRU interface and filters, transforms and dumps the results to a CSV file](https://github.com/sschuepbach/metafacture-examples/tree/master/Swissbib-Extensions/Swissbib-SRU)


### read-kafka

*Acts as a Kafka consumer for Metafacture*

* Implementation: [org.swissbib.linked.mf.source.MfKafkaConsumer](https://github.com/linked-swissbib/swissbib-metafacture-commands/blob/master/src/main/java/org/swissbib/linked/mf/source/MfKafkaConsumer.java)
* In: `java.lang.String`
* Out: `java.lang.Reader`
* Options:
    * topics: Kafka topics (separated by #)
    * groupId: Kafka group identifier


### read-json-object

*Reads in a JSON file and splits it at the end of the root object / array. Preferably used in conjunction with [decode-json](#decode-json)* 

* Implementation: [org.swissbib.linked.mf.io.JsonObjectReader](https://github.com/linked-swissbib/swissbib-metafacture-commands/blob/master/src/main/java/org/swissbib/linked/mf/io/JsonObjectReader.java)
* In: `java.lang.Reader`
* Out: `java.lang.String`

Example: [libadmin entries as Ntriples](https://github.com/sschuepbach/metafacture-examples/tree/master/Swissbib-Extensions/Libadmin-Ntriples)


### split-entitites

*Splits entities into individual records.*

* Implementation: [org.swissbib.linked.mf.pipe.EntitySplitter](https://github.com/linked-swissbib/swissbib-metafacture-commands/blob/master/src/main/java/org/swissbib/linked/mf/pipe/EntitySplitter.java)
* In: [org.culturegraph.mf.framework.StreamReceiver](https://github.com/culturegraph/metafacture-core/blob/master/src/main/java/org/culturegraph/mf/framework/StreamReceiver.java)
* Out: [org.culturegraph.mf.framework.StreamReceiver](https://github.com/culturegraph/metafacture-core/blob/master/src/main/java/org/culturegraph/mf/framework/StreamReceiver.java)
* Option: `entityBoundary`: Node depth for entity splitting

Example: [linked-swissbib "Baseline"](https://github.com/sschuepbach/metafacture-examples/tree/master/Swissbib-Extensions/Linked-Swissbib-Baseline)


### update-es-id

*Identifies partially modified documents by comparing them to an Elasticsearch index. Is tailored to the so-called baseline workflow of linked-swissbib, so it's probably useless for other purposes*

* Implementation: [org.swissbib.linked.mf.pipe.ESIdUpdate](https://github.com/linked-swissbib/swissbib-metafacture-commands/blob/master/src/main/java/org/swissbib/linked/mf/pipe/ESIdUpdate.java)
* In: [org.culturegraph.mf.framework.StreamReceiver](https://github.com/culturegraph/metafacture-core/blob/master/src/main/java/org/culturegraph/mf/framework/StreamReceiver.java)
* Out: [org.culturegraph.mf.framework.StreamReceiver](https://github.com/culturegraph/metafacture-core/blob/master/src/main/java/org/culturegraph/mf/framework/StreamReceiver.java)
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


### write-csv

*Serialises data as CSV file with optional header*

* Implementation: [org.swissbib.linked.mf.writer.ContinuousCsvWriter](https://github.com/linked-swissbib/swissbib-metafacture-commands/blob/master/src/main/java/org/swissbib/linked/mf/writer/ContinuousCsvWriter.java)
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

Examples: [Workflow which queries the Swissbib SRU interface and filters, transforms and dumps the results to a CSV file](https://github.com/sschuepbach/metafacture-examples/tree/master/Swissbib-Extensions/Swissbib-SRU)


### write-esbulk

*Writes records as JSON files which comply with the requirements of the Bulk API of Elasticsearch.*

* Implementation: [org.swissbib.linked.mf.writer.ESBulkWriter](https://github.com/linked-swissbib/swissbib-metafacture-commands/blob/master/src/main/java/org/swissbib/linked/mf/writer/ESBulkWriter.java)
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

Example: [linked-swissbib "Baseline"](https://github.com/sschuepbach/metafacture-examples/tree/master/Swissbib-Extensions/Linked-Swissbib-Baseline)


### write-kafka

*Acts as a producer in a Kafka cluster.*

* Implementation: [org.swissbib.linked.mf.writer.KafkaWriter](https://github.com/linked-swissbib/swissbib-metafacture-commands/blob/master/src/main/java/org/swissbib/linked/mf/writer/KafkaWriter.java)
* In: `java.lang.Object`
* Out: `java.lang.Void`
* Options:
    * host: Hostname of Kafka cluster (required)
    * port: Port of Kafka cluster (required)
    * topic: Name of Kafka topic (required)

Example: [A very small example of using the Kafka consumer](https://github.com/sschuepbach/metafacture-examples/tree/master/Swissbib-Extensions/Kafka-Producer)


### write-neo4j

*Writes csv files for batch uploading to a new Neo4j database. Intended to be used in junction with index-neo4j.*

* Implementation: [org.swissbib.linked.mf.writer.NeoWriter](https://github.com/linked-swissbib/swissbib-metafacture-commands/blob/master/src/main/java/org/swissbib/linked/mf/writer/NeoWriter.java)
* In: `java.lang.Object`
* Out: `java.lang.Void`
* Options:
    * csvDir: Path to the output directory
    * csvFileLength: Numbers of records in one dedicated CSV file
    * batchWriteSize: Maximal number of records of the same category

Example: [Graph visualisation of the GND](https://github.com/guenterh/gndHackathon2016/blob/master/examples/gh/hackathonGND)


### write-rdf-1line

*Writes RDF-XML files, one line per record.*

* Implementation: [org.swissbib.linked.mf.writer.SingleLineWriterRDFXml](https://github.com/linked-swissbib/swissbib-metafacture-commands/blob/master/src/main/java/org/swissbib/linked/mf/writer/SingleLineWriterRDFXml.java)
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

Example: [Deprecated linked-swissbib "baseline" for bibliographicResource documents (use resourceTransformation.rdfXml.flux)](https://github.com/linked-swissbib/mfWorkflows/tree/unused-transformations/src/main/resources/transformation/resource)


### write-socket

*Sets up a socket server*

* Implementation: [org.swissbib.linked.mf.writer.SocketWriter](https://github.com/linked-swissbib/swissbib-metafacture-commands/blob/master/src/main/java/org/swissbib/linked/mf/writer/SocketWriter.java)
* In: `java.lang.Object`
* Out: `java.lang.Void`
* Option: `port`: Port of socket server (host is always 127.0.0.1)

Example: [Stream MARC-XML to socket](https://github.com/sschuepbach/metafacture-examples/tree/master/Swissbib-Extensions/Socket-Sink)
