Jnomics
=======
Jnomics is a cloud-scale sequence analysis suite designed to help meet 
the computational challenges presented by the continuing revolution in 
massively parallel DNA sequencing technologies.


Building
=======
To Build Jnomics run
 ./build.py compile

To Build the Jnomics jar file
 ./build.py jar

A jnomics-*.jar file will be created.

The build script is not very smart,
if changes to the code are made you will 
want to run both compile and jar again
to make sure all changes end up in the
jar file.