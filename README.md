# swissbib-metafacture-commands
Plugin with additional Metafacture commands

## List of commands

### decode-ntriples
`org.swissbib.linked.mf.decoder.NtriplesDecoder`

*Parses Ntriples-decoded records.*

Parameter:
*`unicodeEscapeSeq`: "true", "false"

### encode-esbulk
`org.swissbib.linked.mf.pipe.NtriplesEncoder`

*Encodes records for bulk uploading to Elasticsearch.*

### encode-neo4j
`org.swissbib.linked.mf.pipe.NeoEncoder`
*Encodes records as csv files for batch uploading them to a new Neo4j-database. As the headers of the csv files are hardcoded, it is not ready to be used in a broader context.*

### index-esbulk
`org.swissbib.linked.mf.pipe.ESBulkIndexer`

*Indexes records in Elasticsearch.*

Parameters:

*esClustername: Elasticsearch cluster name
*recordsPerUpload: Number of records per single bulk upload
*esNodes: Elasticsearch nodes. Nodes are separated by #

### index-neo4j
`org.swissbib.linked.mf.writer.NeoIndexer`

*Indexes fields in Neo4j. Because the selection of the fields which are to be indexed is hardcoded, the benefit of this command is somewhat limited.*

Parameters:

*batchSize: Size of batch upload for Neo4j
*dbDir: Path to Neo4j database

### itemerase-es
`org.swissbib.linked.mf.pipe.ESItemErase`

*Deletes items which belong to a certain bibliographicResource. Recommended for internal use only.*

Parameters:

*esClustername: Elasticsearch cluster name
*esNodes: Elasticsearch nodes. Nodes are separated by #
*esIndex: Elasticsearch index
*esType: Document type for Elasticsearch

### lookup-es
`org.swissbib.linked.mf.pipe.ESLookup`

*Filters out records whose identifier already exists in an Elasticsearch index.*

Parameters:

*esClustername: Elasticsearch cluster name
*esNodes: Elasticsearch nodes. Nodes are separated by #
*esIndex: Elasticsearch index
*esType: Document type for Elasticsearch

### split-entitites
`org.swissbib.linked.mf.pipe.EntitySplitter`

*Splits entities into individual records.*

Parameter:

*entityBoundary: Node depth for entity splitting

### update-es-id
`org.swissbib.linked.mf.pipe.ESIdUpdate`

*Identifies partially modified documents by comparing them to an Elasticsearch index.*

Parameter:

*esClustername: Elasticsearch cluster name
*esNodes: Elasticsearch nodes. Nodes are separated by #
*esIndex: Elasticsearch index
*esType: Document type for Elasticsearch
*matchingFields: Fields which should be matched. # is delimiter.
*sThreshold: Matching threshold
*refPath:
*uriPrefix:
*graphDbDir: Path to Neo4j database

### write-esbulk
`org.swissbib.linked.mf.writer.ESBulkWriter`

*Writes records as JSON files which comply with the requirements of the Bulk API of Elasticsearch.*

Parameters:

*outDir: Root directory for output
*filePrefix: Prefix for file names
*fileSize: Number of records in one file
*subdirSize: Number of files in one subdirectory (Default: 300)
*jsonCompliant: Should files be JSON compliant (Boolean; default: false)? Warning: Setting this parameter to true will result in an invalid Bulk format!
*compress: Should files be .gz-compressed? (Default is true)
*type: Type name of records (will only be attached to filename)

### write-neo4j
`org.swissbib.linked.mf.writer.NeoWriter`

*Writes csv files for batch uploading to a new Neo4j database. Intended to be used in junction with index-neo4j.*

Parameters:

*csvDir: Path to the output directory
*csvFileLength: Numbers of records in one dedicated CSV file
*batchWriteSize: Maximal number of records of the same category


### write-rdf-1line
`org.swissbib.linked.mf.writer.SingleLineWriterRDFXml`

*Writes RDF-XML files, one line per record.*

Parameters:

*usecontributor: "true", "false"
*rootTag: XML root tag
*extension: File extension for output files
*compress: Should output files be compressed? ("true", "false")
*baseOutDir: Base directory for output files:
*outFilePrefix: Prefix for output files
*fileSize: Number of records in one file
*subDirSize: Number of records in one subdirectory
*type: Concept / type name
