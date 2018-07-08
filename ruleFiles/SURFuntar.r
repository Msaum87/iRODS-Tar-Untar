############################
#Author: Matthew Saum
#Copyright 2018 SURFsara BV
#Apache License 2.0
############################
#This file is a regular rule file,
#and can be run with an iRule command.
#"irule -F SURFuntar.r "*Tar='/zone/coll/tarFile.SURFtape'"
#If no tarfile is specified, my default one from
#the INPUT line will be used, or attempted anyway.

#VERSIONING
#v1.1- Added a RegExp scrubber for the metadata tags
#-----------
#TO DO
#-----------
#I am sure I am missing something here.

SURFuntar(){
 #*Tar will be our target TAR file for processing.

 #Resource to unpack the tarball on
 *Resc="demoResc";
 #Need to separate the Collection and Data Object Name into two values.
 msiSplitPath(*Tar, *Coll, *tData);
 
 #The name of the table of contents / manifest file you generated
 *tocFile="manifest.txt";
 
 #This exception here will prevent us from checking checksums in data that was not tarballed
 #This includes: the tarball itself, the checksum file itself, and a meta-data.xml file
 *excludeFile="FileGen.log";



 #Step 1, untar the collection after moving it to the right resource
  msiDataObjPhymv(*Tar, *Resc, "null", "", "null", *stat);  
 #Due to the a bug, it appears that tar-balls over 4GB or so fail. 
 #Reports Vary based on systems. This next line is for when this bug is fixed.
 #msiTarFileExtract(*Tar, *Coll, *Resc, *Stat);

 #Until then, we need to call an msiExecCmd to manually untar it at a unix level
 #And then register the items found.
 foreach(*tarball in SELECT DATA_PATH where DATA_NAME = *Tar){
  msiExecCmd("untar", "*tarball", "", "", "", "*result");
  writeLine("stdout", "Unpacked *tarball.");
 }
 writeLine("stdout","Unpacking TAR file to "++*Coll);

 #Step 2, validate the checksums of the files.
 #Opens our checksum file. Currently set to 10000 bytes.
 *CheckSums=trimr(*tData, ".tar")++".cksums"

 if(ifExists(*Coll++"/"++*CheckSums)== 1){
    msiDataObjOpen(*Coll++"/"++*CheckSums,*CKsums);
    msiDataObjRead(*CKsums, 2^31-1, *file_BUF);
      
  
    #This block of code will convert our buff to a string for manipulation.
    #We trim the leading "." and everything after the "::"
    #Then we run a check if the file exists in the collection.
    #If a file is not found, the process is halted and exits
    msiBytesBufToStr(*file_BUF, *count_STR);
    *countList=split(*count_STR,"\n");
    foreach(*cnt in *countList){
      if(ifExists(*Coll++trimr(triml(*cnt,"\."),"::"))!=1){ 
        writeLine("stdout","WARNING: DID NOT FIND "++*cnt);
        msiExit("-1","WARNING! NOT ALL FILES EXPECTED IN TARBALL WERE EXTRACTED. HALTING PROCESS!");
      }
    }
  
    #To prevent the searching of similarily named collections (such as ~/FileGen and ~/FileGeneration)
    #We have to search twice, once for the precise collection and another with 
    foreach(*row in SELECT 
                           DATA_NAME, 
                           COLL_NAME 
                    WHERE 
                           COLL_NAME = *Coll 
     ){
     #Our logical iRODS Path
     *ipath=*row.COLL_NAME++"/"++*row.DATA_NAME;
     #Our relative path to the tar collection
     *rpath="."++triml(*ipath, *Coll);
     #Scrubs the RegExp for special chars, escaping them.
     *scrubbed=scrubRE(*rpath);
     #Checks our new checksum of each file from the tarball
     msiDataObjChksum(*ipath, "forceChksum=", *new);
     #Builds our Tag Structure for filtering meta-data out of a bytes-buffer
     msiStrToBytesBuf("<PRETAG>*scrubbed::</PRETAG>*rpath<POSTTAG>\n</POSTTAG>", *tag_BUF);
     msiReadMDTemplateIntoTagStruct(*tag_BUF, *tags);
     #Takes our Tag Structure and searches the opened checksum manifest for a match
     msiExtractTemplateMDFromBuf(*file_BUF, *tags, *cKVP);
     #Convernts our result into a string useable for operations.
     *old=triml(str(*cKVP),*rpath++"=");
     #Excluding the two files 
     if(*old != *new 
     && *row.DATA_NAME != *tData 
     && *row.DATA_NAME != *tocFile 
     && *row.DATA_NAME != *excludeFile
     && *row.DATA_NAME != *CheckSums
     ){
        writeLine("stdout","WARNING!!!\n"++*rpath++" does not have a matching checksum to our records! This is bad.");
     }
     else{
        writeLine("stdout","Checksum for "++*rpath++" is good.");
     }
    } 
    #Our recursive search to deal with the subdirectories
    *CollRec=*Coll++"/%";
    foreach(*row in SELECT 
                           DATA_NAME, 
                           COLL_NAME 
                    WHERE 
                           COLL_NAME like *CollRec 
     ){
     #Our logical iRODS Path
     *ipath=*row.COLL_NAME++"/"++*row.DATA_NAME;
     #Our relative path to the tar collection
     *rpath="."++triml(*ipath, *Coll);
     #Scrubs the RegExp for special chars, escaping them.
     *scrubbed=scrubRE(*rpath);
     #Checks our new checksum of each file from the tarball
     msiDataObjChksum(*ipath, "forceChksum=", *new);
     #Builds our Tag Structure for filtering meta-data out of a bytes-buffer
     msiStrToBytesBuf("<PRETAG>*scrubbed::</PRETAG>*rpath<POSTTAG>\n</POSTTAG>", *tag_BUF);
     msiReadMDTemplateIntoTagStruct(*tag_BUF, *tags);
     #Takes our Tag Structure and searches the opened checksum manifest for a match
     msiExtractTemplateMDFromBuf(*file_BUF, *tags, *cKVP);
     #Convernts our result into a string useable for operations.
     *old=triml(str(*cKVP),*rpath++"=");
     #Excluding the two files 
     if(*old != *new 
     && *row.DATA_NAME != *tData 
     && *row.DATA_NAME != *tocFile 
     && *row.DATA_NAME != *excludeFile
     && *row.DATA_NAME != *CheckSums
     ){
        writeLine("stdout","WARNING!!!\n"++*rpath++" does not have a matching checksum to our records! This is bad.");
     }
     else{
        writeLine("stdout","Checksum for "++*rpath++" is good.");
     }
    }        
 msiDataObjClose(*CKsums, *stat);
 msiDataObjUnlink("objPath="++*Coll++"/"++*CheckSums++"++++forceFlag=", *stat2);
 writeLine("stdout","Deleted checksums file "++*Coll++"/"++*CheckSums);
 }

 
 #Step 4, we remove the original tarball.
 #Forceflag will prevent trash holdings
 msiDataObjUnlink("objPath="++*Tar++"++++forceFlag=", *stat);
 msiDataObjUnlink("objPath="++*Coll++"/"++*tocFile++"++++forceFlag=", *stat2);
 writeLine("stdout","Deleting original tarball "++*Tar);
 writeLine("stdout","Deleted manifest "++*Coll++"/"++*tocFile);
}


#This is the RegExp scrubber for our tag structure
scrubRE(*i){
  *output = "";
  # This is a comma separated list of characters to escape.
  # Keep the \ symbol first to prevent clipping later added escapes.
  # In addition to regexp, I also included a space.
  # Everything within the pairs of backticks (``) is treated as a string
  *chars =``\,^,$,{,},[,],(,),.,*,+,?,|,<,>,-, ,&,``;
  *charList = split(*chars, ",");
  # Loop through the input string
  for (*pos = 0; *pos < strlen(*i); *pos = *pos + 1) {
     *c = substr(*i, *pos, *pos + 1);
     *o = *c;
     foreach(*char in *charList){
      if ( *char == *c ) {
        *o = "\\" ++ *c;
        break;
      }
     }
     *output = *output ++ *o;
  }
  # Return our escaped string
  "*output";
} #scrubRE


#Matthew's basic file-existance checker function.
#Checks if a file exists
#*i is a full file path "/tempZone/home/rods/testfile.dat" or so
#Returns 0 if no file, 1 if file found.
ifExists(*i){
 *b = 0;
 msiSplitPath(*i, *coll, *data);
 foreach(*row in SELECT 
                        COLL_NAME, 
                        DATA_NAME 
                 WHERE 
                        COLL_NAME = '*coll' 
                    AND DATA_NAME = '*data'
    ){
    *b = 1;
    break;
 }
 *b;
}

INPUT *Tar="/SURF/home/irsara/FileGen/FileGen.tar"
OUTPUT ruleExecOut
