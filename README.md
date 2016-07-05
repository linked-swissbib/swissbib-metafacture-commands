# swissbib-metafacture-commands
Plugin with additional Metafacture commands

## List of commands

### decode-ntriples
*Parses Ntriples-decoded records.*

### encode-esbulk
*Encodes records for bulk uploading to Elasticsearch.*

### encode-neo4j
*Encodes records as csv files for batch uploading them to a new Neo4j-database. As the headers of the csv files are hardcoded, it is not ready to be used in a broader context.*

### index-esbulk
*Indexes records in Elasticsearch.*

### index-neo4j
*Indexes fields in Neo4j. Because the selection of the fields which are to be indexed is hardcoded, the benefit of this command is somewhat limited.*

### itemerase-es
*Deletes items which belong to a certain bibliographicResource. Recommended for internal use only.*

### lookup-es
*Filters out records whose identifier already exists in an Elasticsearch index.* 

### split-entitites
*Splits entities into individual records.*

### update-es-id
*Identifies partially modified documents by comparing them to an Elasticsearch index.*

### write-esbulk
*Writes records as JSON files which comply with the requirements of the Bulk API of Elasticsearch.*

### write-neo4j
*Writes csv files for batch uploading to a new Neo4j database. Intended to be used in junction with index-neo4j.*

### write-rdf
*Writes RDF-XML files.*

### write-rdf-1line
*Writes RDF-XML files, one line per record.*
