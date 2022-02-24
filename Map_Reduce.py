import re, pymp, time

Word_List = ["hate", "love", "death", "night", "sleep", "time",
"henry", "hamlet", "you", "my", "blood", "poison", "macbeth",
"king", "heart", "honest"]

"""
Function to split file into slist of words
"""
def load_files(filename):
    with open("./map-reduce-aflores007/"+filename, 'r') as file:
        words_in_file = [word for line in file for word in line.split()]
    return words_in_file

"""
Function to count total instances of words in 8 files
"""
def count_words(listOfWords):
    listRet = pymp.shared.list()
    dictRet = pymp.shared.dict() #If i initialize shared data struc inside parallel section, output is {}
    sum = 1 #Start counting from 1, since first instance is met
    for x in range(1,9):
        shakespeare = load_files("shakespeare"+str(x)+".txt")
        start = time.perf_counter()
        with pymp.Parallel() as p: #ENter parallel section
            update_lock = p.lock
            for i in p.iterate(listOfWords): #Iterate through the list of list of words ^
                for j in shakespeare: #iterate through shakespeare files
                    if j.lower() == i.lower(): #if words match
                        update_lock.acquire() #Acquire lock
                        sum+=1 #update sum
                        update_lock.release() #Release lock
                update_lock.acquire()
                dictRet[i]=sum #edit shared dictionary
                update_lock.release()
        end = time.perf_counter()
        print("Total time to iterate and find matches: ", end-start)
    return dictRet

def main():
    start = time.perf_counter()
    result = count_words(Word_List)
    end = time.perf_counter()
    print(result)
    print("Total time of whole computation (including loading files): ", end-start)
    return

if __name__ == "__main__":
    main()
