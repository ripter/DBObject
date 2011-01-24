echo Update the Revision Number
cd "C:\Users\Administrator\Documents\Visual Studio 2010\Projects\DBObject2\DBObject2\"
UpdateVersion.exe -r Increment -v File -i "Properties\AssemblyInfo.cs" -o "Properties\AssemblyInfo.cs"

echo Building
C:\WINDOWS\Microsoft.NET\Framework\v3.5\Csc.exe @"C:\Users\Administrator\Documents\Visual Studio 2010\Projects\DBObject2\scripts\debug.rsp"
C:\WINDOWS\Microsoft.NET\Framework\v3.5\Csc.exe @"C:\Users\Administrator\Documents\Visual Studio 2010\Projects\DBObject2\scripts\release.rsp"

echo Building Documentation
doxygen "C:\Users\Administrator\Documents\Visual Studio 2010\Projects\DBObject2\Doxyfile"

echo Building the help file
"%programfiles%\HTML Help Workshop\hhc" "C:\Users\Administrator\Documents\Visual Studio 2010\Projects\DBObject2\html\index.hhp"

