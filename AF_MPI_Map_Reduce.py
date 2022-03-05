"""
Airam FLores
Assignment 3 MPI
"""
from mpi4py import MPI
import re, time
#get world communicator
comm = MPI.COMM_WORLD

#get process # (rank)
rank = comm.Get_rank()

#get num of processes
size = comm.Get_size()

#Global List of words to check
ShakespeareWordList = ["hate", "love", "death", "night", "sleep", "time",
"henry", "hamlet", "you", "my", "blood", "poison", "macbeth",
"king", "heart", "honest"]

#Global List of shakespeare files to open
File_List = ["shakespeare1.txt","shakespeare2.txt","shakespeare3.txt","shakespeare4.txt","shakespeare5.txt","shakespeare6.txt","shakespeare7.txt","shakespeare8.txt"]

#Dict to append  ouccurrences on all files listed
Result_Dict = {'hate':0,'love':0,'death':0,'night':0,'sleep':0,'time':0,'henry':0,'hamlet':0,
'you':0,'my':0,'blood':0,'poison':0,'macbeth':0,'king':0,'heart':0,'honest':0}

#To append name of files assigned to processes
localList = []

if rank is 0:
    print("Thread 0 is now distributing")

    #Elements per process
    docsPerThread = len(File_List) / size

    localList = File_List[:int( docsPerThread )]

    #Distributing files/work among processes
    for process in range(1, size):

        #start and end of slice we're sending
        startOfSlice = int( docsPerThread * process )
        endOfSlice = int( docsPerThread * (process + 1) )
        sliceToSend = File_List[startOfSlice:endOfSlice]
        comm.send(sliceToSend, dest=process, tag=0)


else:
    # receive a message from thread 0 with tag of 0
    localList = comm.recv(source=0, tag=0)

print(f'Thread {rank} received: {localList}')

#Looping through the list of files assigned to each processe
for files in localList:
    with open("./map-reduce-aflores007/"+files, 'r') as file:
        print("*********************************************")
        print(f'Thread {rank} is now working on {files}')
        print("*********************************************")

        words_in_file = re.findall(r"[A-Za-z]+(?:['`][A-Za-z]+)*",file.read())
        for word in words_in_file:
                if word.lower() in ShakespeareWordList:
                    Result_Dict[word.lower()] = Result_Dict[word.lower()]+1


print(f'Thread {rank} found: {Result_Dict}')

#List of all dictionaries collected
temp = comm.gather(Result_Dict,root=0)


if rank == 0:
    #Uncomment to see list of all dictionaries
    #print("gather():" , temp)
    start = time.perf_counter()
    final_res = {'hate':0,'love':0,'death':0,'night':0,'sleep':0,'time':0,'henry':0,'hamlet':0,
    'you':0,'my':0,'blood':0,'poison':0,'macbeth':0,'king':0,'heart':0,'honest':0}

    #Looping through dictionaries and adding them to the final dictionary
    for index in range(len(temp)):
        for key in temp[index]:
            final_res[key]+=temp[index][key]

    print("Overall Count: ", final_res)
    end = time.perf_counter()
    print("Total time of Thread 0 to sum all ouccurrences: ", end-start)
