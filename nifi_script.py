from org.apache.nifi.processors.script import ExecuteScript
from org.apache.nifi.processor.io import InputStreamCallback
from java.io import BufferedReader, InputStreamReader

class ReadFirstLine(InputStreamCallback) :
    __line = None;

    def __init__(self) :
        pass

    def getLine(self) :
        return self.__line

    def process(self, input) :
        try :
            reader = InputStreamReader(input)
            bufferedReader = BufferedReader(reader)
            self.__line = bufferedReader.readLine()
        except :
            print "Exception in Reader:"
            print '-' * 60
            traceback.print_exc(file=sys.stdout)
            print '-' * 60
            raise
        finally :
            if bufferedReader is not None :
                bufferedReader.close()
            if reader is not None :
                reader.close()

flowFile = session.get()
if flowFile is not None :

    reader = ReadFirstLine()
    session.read(flowFile, reader)
    flowFile = session.putAttribute(flowFile, "from-content", reader.getLine()[:2])
    session.transfer(flowFile, ExecuteScript.REL_SUCCESS)
