outdir      = FLUX_DIR;
filesize    = "10000";
records     = "20000";
index       = "lsb";
compress    = "false";
extension   = "jsonld";

FLUX_DIR + "swissbibMarc.xml"|
open-file|
decode-xml|
handle-marcxml|
filter(FLUX_DIR + "245aFilter.xml")|
morph(FLUX_DIR + "resourceMorph.xml")|
change-id|
encode-esbulk(escapeChars="true", header="true", index=index, type="bibliographicResource")|
write-esbulk(baseOutDir=outdir, fileSize=filesize, jsonCompliant="true", type="bibliographicResource", compress=compress, extension=extension);
