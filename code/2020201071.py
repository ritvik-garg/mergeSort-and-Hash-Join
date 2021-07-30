import sys
import math
import heapq
import time 
import os
import ntpath
from pathlib import Path

col_isto_size = {}
col_index = {}
heap = []


class HeapNode:
    sort_col = []
    sort_order = None
    def __init__(self):
        self.data = []
        self.file_ptr = 0


    def __lt__(self, other):
        return compare(self.data, other.data, HeapNode.sort_col, HeapNode.sort_order)


def delete_temp_files(temp_filenames):
    for filename in temp_filenames:
        os.remove(filename)


def checkRec(temp_filenames):
    n = len(temp_filenames)
    res_len = []
    for i in range(n):
        f = open(temp_filenames[i])
        lines = f.readlines()
        res_len.append(len(lines))
    return res_len

def compare(l1,l2,columns,flag):
    if(flag):
        for j in columns:
            i=col_index[j]
            if(l1[i]>l2[i]):
                return True
            elif(l1[i]==l2[i]):
                continue
            else:
                return False
        return False
    else:
        for j in columns:
            i=col_index[j]
            if(l1[i]<l2[i]):
                return True
            elif(l1[i]==l2[i]):
                continue
            else:
                return False
        return False


def mergeFiles(temp_filenames, output_file, sort_acc_to_cols, order_flag):
    # print ("sorting all files data ")
    filepointer=[None]*len(temp_filenames)
    HeapNode.sort_col = sort_acc_to_cols
    HeapNode.sort_order = order_flag
    for i in range(len(temp_filenames)):
        filepointer[i]=open(temp_filenames[i])
        line = filepointer[i].readline()
        # print ("line in loop : ", line)
        if line :
            data = split_line(line)
        else:
            data = []
        temp=HeapNode()
        temp.file_ptr=i
        temp.data=data
        heap.append(temp)

    heapq.heapify(heap)

    opFilePtr = open(output_file, 'w')
    num_tempFiles = len(temp_filenames)
    fileRead=0
    num_records=0
    while(fileRead!=num_tempFiles):
        top = heapq.heappop(heap)
        final_data = []
        final_data.append(top.data)
        writeToFile(opFilePtr, final_data)
        num_records+=1
        line = filepointer[top.file_ptr].readline()
        if line :
            new_data = split_line(line)
        else:
            new_data = []
        
        if(len(new_data)!=0):
            top.data = new_data
            heap.append(top)
            heapq.heapify(heap)
        else:
            # print("file read : ", fileRead)
            fileRead+=1
    
    # print("all files done : ", num_records)
    for i in range(len(temp_filenames)):
         filepointer[i].close()
    
    opFilePtr.close()
    # print ("writing sorted all files data into output file ")
    return         


def writeToFile(fptr, data):
    
    # print ("data to write : ", data)
    # return
    for line in data:
        temp=""
        for word in line:
            temp += (word + " ")
        temp = temp[:-1]
        # temp+="\n"
        fptr.write(temp)
        


def sortOn(x, sort_acc_to_cols):
    res = []
    # print ("x : ", x)
    # print ("col index :", col_index)
    for col in sort_acc_to_cols:
        res.append(x[col_index[col]])
    return res

def getSortedData(data, order_flag, sort_acc_to_cols):
    # print ("data :", data)
    return sorted(data, key=lambda x : sortOn(x, sort_acc_to_cols), reverse = order_flag)

def split_line(line):
    # print ("line to splot : ", line)
    # print ("col is to size dict : ", col_isto_size)
    len_of_cols = col_isto_size.values()
    # print ("dict vales : ,", len_of_cols)
    start_index=0
    words = []

    for col_len in len_of_cols:
        words.append(line[start_index:start_index+col_len])
        start_index = start_index+col_len+1

    return words


def split_sort_storefile(input_file, order_flag, chunksize, sort_acc_to_cols):
    input_f = open(input_file)
    input_file_line = input_f.readline()
    # print("1st line of input file : ", input_file_line)
    temp_filenames = []
    temp_data = []
    tempfile_index = 0
    num_lines = 0

    while(input_file_line):
        # print ("input file name : ", input_file_line)
        words = split_line(input_file_line)
        # print ("words : ", words)
        temp_data.append(words)
        # print ("te mp data : ", temp_data)
        input_file_line = input_f.readline()
        num_lines+=1

        if(num_lines==chunksize):
            temp_filename = str(tempfile_index)+'.txt'
            # print ("Creating temp file : ", temp_filename)
            temp_filenames.append(temp_filename)
            tempfile = open(temp_filename, 'w')
            # print ("sorted temp file : ", tempfile_index+1)
            # print ("sort this data : ", temp_data)
            sorted_tempdata = getSortedData(temp_data, order_flag, sort_acc_to_cols)
            # print ("writing sorted data into temp file : ", tempfile_index+1)
            # print ("sorted data on yhis file : ", sorted_tempdata)
            # break
            writeToFile(tempfile, sorted_tempdata)
            temp_data= []
            tempfile_index+=1
            num_lines=0
            tempfile.close()

    if(num_lines > 0):
        temp_filename = str(tempfile_index)+'.txt'
        # print ("Creating temp file : ", temp_filename)
        temp_filenames.append(temp_filename)
        tempfile = open(temp_filename, 'w')
        # print ("sorted temp file : ", tempfile_index+1)
        sorted_tempdata = getSortedData(temp_data, order_flag, sort_acc_to_cols)
        # print ("writing sorted data into temp file : ", tempfile_index+1)
        writeToFile(tempfile, sorted_tempdata)
        temp_data= []
        tempfile_index+=1
        num_lines=0
        tempfile.close()

    input_f.close()
    return temp_filenames


def getTupleSize():
    col_size = col_isto_size.values()
    num_cols = len(col_size)
    return sum(col_size)+(num_cols-1)*2+2

def getTotalNumOfRecords(filename):
    f = open(filename)
    lines = f.readlines()
    f.close()
    return len(lines)


def read(input_fielname, output_filename, R_flag, max_tuples_in_mem):
    input_file = input_fielname
    output_file = output_filename
    f = open(input_fielname)
    lines = f.readlines()
    f.close()
    tuple_size = len(lines[0])
    # print ("tuple size of input file : ", tuple_size)
    global col_isto_size
    global col_index
    if R_flag:
        x_data, y_data = lines[0].split(' ')
        col_index_R = {}
        col_size_R = {}
        col_size_R["X"] = len(x_data)
        col_index_R["X"] = 0
        col_size_R["Y"] = len(y_data)
        col_index_R["Y"] = 1
        col_isto_size = col_size_R
        col_index = col_index_R

    else:
        y_data, z_data = lines[0].split(' ')
        col_index_S = {}
        col_size_S = {}
        col_size_S["Y"] = len(y_data)
        col_index_S["Y"] = 0
        col_size_S["Z"] = len(z_data)
        col_index_S["Z"] = 1
        col_isto_size = col_size_S
        col_index = col_index_S

    total_no_records = getTotalNumOfRecords(input_file)
    # print ("total num of records in file : ", total_no_records)
    chunk_size = max_tuples_in_mem
    # print ("chunk size (no of records in temp file ): ", chunk_size)
    # print("#####running phase - 1 ")
    temp_filenames = split_sort_storefile(input_file, order_flag, chunk_size, sort_acc_to_cols)
    # print("#####running phase - 2 ")
    mergeFiles(temp_filenames, output_file, sort_acc_to_cols, order_flag)
    fileop = open(output_file)
    lines = fileop.readlines()
    fileop.close()
    
    op_fptr = open(output_filename)
    count=0

    file_index = 0
    if R_flag:
        fname = "r_" + str(file_index) + ".txt"
    else:
        fname = "s_" + str(file_index) + ".txt"
    fptr = open(fname, 'w')

    l = op_fptr.readline()
    # print ("max tuples :", max_tuples_in_mem)
    while(l):
        if count == max_tuples_in_mem:
            file_index += 1
            count=0
            if R_flag:
                fname = "r_" + str(file_index) + ".txt"
            else:
                fname = "s_" + str(file_index) + ".txt"
            fptr.close()
            fptr = open(fname, 'w')
        count += 1
        fptr.write(l)
        l = op_fptr.readline()
        
    fptr.close()
    # // create sublist
    
    # print("\n###deleting all temp files\n")
    delete_temp_files(temp_filenames)
    # print ("#####\ninputfile now sorted\n######")

    
    return total_no_records, file_index

def write_to_file(fptr, data):
    for d in data:
        fptr.write(d)


def getnext(sortedR_file, sortedS_file, n_tuples_r, n_tuples_s, output_filename):

    op_fptr = open(output_filename, 'w')

    temp_data = []
    mark=None
    
    f1 = open(sortedR_file)
    line1 = f1.readline()
    tuple_size_r = len(line1)
    r_x, r_y = line1.split(" ")
    r_x_size, r_y_size = len(r_x), len(r_y)

    f2 = open(sortedS_file)
    line2 = f2.readline()
    tuple_size_s = len(line2)
    s_y, s_z = line2.split(" ")
    s_y_size, s_z_size = len(s_y), len(s_z)

    r=0
    s=0
    # print ("block size : ", block_size)
    while(r<(n_tuples_r)):
        f1.seek(tuple_size_r*r)
        f2.seek(tuple_size_s*s)
        line1 = f1.readline()
        line2 = f2.readline()
        curr_r_y = line1[r_x_size+1:-1]
        curr_s_y = line2[:s_y_size]

        if(mark==None):

            while(curr_r_y < curr_s_y and r<(n_tuples_r)):
                r+=1
                f1.seek(tuple_size_r*r)
                f2.seek(tuple_size_s*s)
                line1 = f1.readline()
                line2 = f2.readline()
                curr_r_y = line1[r_x_size+1:-1]
                curr_s_y = line2[:s_y_size]

            while(curr_r_y > curr_s_y and s<(n_tuples_s)):
                s+=1
                f1.seek(tuple_size_r*r)
                f2.seek(tuple_size_s*s)
                line1 = f1.readline()
                line2 = f2.readline()
                curr_r_y = line1[r_x_size+1:-1]
                curr_s_y = line2[:s_y_size]

            mark=s


        if(curr_r_y == curr_s_y):
            result = line1[:-1] + line2[s_y_size:]
            temp_data.append(result)
            s+=1
            if(len(temp_data)==block_size):
                write_to_file(op_fptr, temp_data)
                temp_data = []

        else:
            s = mark
            r+=1
            mark=None

    if(len(temp_data)!=0):
        write_to_file(op_fptr, temp_data)
        # print("wrtiting in output file ")
        temp_data = []

    f1.close()
    f2.close()
    op_fptr.close()


def rolling_hash_func(data, M):
    p = 31
    power = 1
    hash_val = 0
    for i in range(len(data)):
        hash_val = ((hash_val + (ord(data[i]) - ord('a') + 1) * power) % M)
        power = (power * p) % M
 
    return int(hash_val)

def close(total_files_r, total_files_s):
    # print ("closing temporary R sublist files ")
    for i in range(total_files_r+1):
        fname = "r_" + str(i) + ".txt"
        my_file = Path(fname)
        if my_file.is_file():
            os.remove(fname)

    # print ("closing temporary S sublist files ")
    for i in range(total_files_s+1):
        fname = "s_" + str(i) + ".txt"
        my_file = Path(fname)
        if my_file.is_file():
            os.remove(fname)

    os.remove("sortedR.txt")
    os.remove("sortedS.txt")    

def read2(r_file, s_file, M):
    f = open(r_file)
    l = f.readline()
    count=0
    while(l):
        r_x = l.split(" ")[0]
        r_y = l.split(' ')[1]
        # print ("r_y : ", r_y)
        hash_value = rolling_hash_func(r_y[:-1], M)
        # if r_x == "acdbc":
        #     print ("r_x gotcha : ", r_y)
        # if r_y == "hhkkh":
        #     print ("its hash value r: ", hash_value)
        #     break
        fname = "hashedR_" + str(hash_value) + ".txt"
        f_new = open(fname, 'a')
        f_new.write(l)
        f_new.close()
        l = f.readline()
        count += 1
    
    # print("total lines in r_file : ", count)
    f.close()

    f = open(s_file)
    l = f.readline()
    count=0
    while(l):
        s_y = l.split(' ')[0]
        
        hash_value = rolling_hash_func(s_y, M)
        # if s_y == "hhkkh":
        #     print ("its hash value s: ", hash_value)
        #     break
        fname = "hashedS_" + str(hash_value) + ".txt"
        f_new = open(fname, 'a')
        f_new.write(l)
        f_new.close()
        l = f.readline()
        count += 1

    # print ("lines in s file  : ", count)
    f.close()




def getnext2(output_filename, M):

    op_fptr = open(output_filename, 'w')

    temp_data = []
    mark=None
    
    for i in range(M):
        fnameR = "hashedR_" + str(i) + ".txt"
        fnameS = "hashedS_" + str(i) + ".txt"

        my_file = Path(fnameR)
        if my_file.is_file():
            f1 = open(fnameR)
        else:
            continue
        n_tuples_r = len(f1.readlines())
        f1.seek(0)
        line1 = f1.readline()
        tuple_size_r = len(line1)
        r_x, r_y = line1.split(" ")
        r_x_size, r_y_size = len(r_x), len(r_y)

        my_file = Path(fnameS)
        if my_file.is_file():
            f2 = open(fnameS)
        else:
            continue
        f2 = open(fnameS)
        n_tuples_s = len(f2.readlines())
        f2.seek(0)
        line2 = f2.readline()
        tuple_size_s = len(line2)
        s_y, s_z = line2.split(" ")
        s_y_size, s_z_size = len(s_y), len(s_z)

        f1.seek(0)
        l1 = f1.readline()
        temp_data = []
        faltu=0
        tempo=0
        while(l1):
            curr_r_y = l1[r_x_size+1:-1]
            curr_r_x = l1[:r_x_size]
            f2.seek(0)
            l2 = f2.readline()
            faltu=0
            # print ("line 1 : ", l1[:-1])

            while(l2):
                # print ("line 2 : ", l2[:-1])
                curr_s_y = l2[:s_y_size]
                curr_s_z = l2[s_y_size+1:-1]
                # print ("curr _s_y : ", curr_s_y)
                # print ("curr _r_y : ", curr_r_y)
                # if curr_r_y == "jilhl" :#and curr_r_x == "aeabd":
                #     print ("curr_s_y : ", curr_r_x)
                # if curr_s_y == "jilhl" and curr_r_y==curr_s_y:
                #     print ("l2 : ", l2)
                #     faltu+=1
                if(curr_r_y == curr_s_y):
                    # print ("macthed line : ")
                    # print ("heheh")
                    result = l1[:-1] + l2[s_y_size:]
                    temp_data.append(result)
                    if(len(temp_data)==block_size):
                        # print ("gotcha")
                        write_to_file(op_fptr, temp_data)
                        temp_data = []
                l2 = f2.readline()
            # print ("falru : ", faltu)
            # print ("\n\n")
            l1 = f1.readline()
            # if faltu!=0:
            #     print ("faltu : ", faltu)
        # print ("tempo : ", tempo)
        if(len(temp_data)!=0):
            write_to_file(op_fptr, temp_data)
            # print("wrtiting in output file ")
            temp_data = []

        f1.close()
        f2.close()

    op_fptr.close()


def close2(M):
    # print ("closing M1 hashed sublists for R and S ")
    for i in range(M):
        fname = "hashedR_" + str(i) + ".txt"
        my_file = Path(fname)
        if my_file.is_file():
            os.remove(fname)

        fname = "hashedS_" + str(i) + ".txt"
        my_file = Path(fname)
        if my_file.is_file():
            os.remove(fname)


if __name__== "__main__":
    start_time = time.time()
    # print ("sys.argv : ", sys.argv)
    n_args = len(sys.argv)

    if n_args != 6:
        print("insufficient command line args\n")
        sys.exit()

    filepathR = sys.argv[1]
    filepathS = sys.argv[2]

    joinType = sys.argv[3]
    M = int(sys.argv[4]) # n_blocks
    
    block_size = 100

    max_tuples_in_mem = M*block_size
    # print ("M : ", M)
    # print ("block size  ", block_size)

    # print ("### starting execution")
    sort_acc_to_cols = ["Y"]
    order_flag = False

    filename_r = ntpath.basename(filepathR)
    filename_s = ntpath.basename(filepathS)
    
    # print ("filename from filepath : ", filename_r, " and ", filename_s)

    # output_filename = filename_r[:-4] + "_" + filename_s[:-4] + "_join.txt"
    output_filename = filename_r + "_" + filename_s + "_join.txt"

    if joinType=="sort":

        # """
        # sort R
        input_file = filepathR
        output_file_R = "sortedR.txt"
        n_tuples_r, total_files_r = read(input_file, output_file_R, True, max_tuples_in_mem)
        # n_tuple   s_r = 50000

        # sort S
        input_file = filepathS
        output_file_S = "sortedS.txt"
        n_tuples_s, total_files_s = read(input_file, output_file_S, False, max_tuples_in_mem)
        # n_tuples_s = 50000
        # """

        getnext(output_file_R, output_file_S, n_tuples_r, n_tuples_s, output_filename)
        close(total_files_r, total_files_s)
    else:

        read2(filepathR, filepathS, M)
        getnext2(output_filename, M)
        close2(M)

    # print ("time taken : ", time.time()-start_time)




    # comm -3 <(sort -u outputFile) <(sort -u inputR_inputS_join.txt)
    

    # export SPAN=/home/ritvik/Desktop/ALL_SUBJECTS/sem_2/sns/a-3/span-avispa/span
    # export AVISPA_PACKAGE=/home/ritvik/Desktop/ALL_SUBJECTS/sem_2/sns/a-3/span-avispa/span

    # 846236
    # 421217