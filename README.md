### Python3 Multiprocessing Workflow Framework 

This framework implements a workflow framework based on Python3 multiprocessing libary. There are many programs can be broken down into simple steps and executed in parrallel, it would be easier to maintain each simple step and more efficient to run by multiple processes.


1. In the workflow, each step can be configured to run in multiple processes.

2. User only needs to write code to implement each workflow step, no need to worry about handling multiprocessing.

3. Real-time monitor of the workflow execution.

4. Platform independent. 



##### Configuration Example:

Here is a sample workflow step config in json:
```  
#==================================================================================
 {
    "dependency"    : "",       # --> dependency workflow step
                                # --> if empty, this step will start immediately.
     "join_wait"     : False,   # --> if False, the step will start immediately, if dependency is 
                                #     empty, this settign is ignored.
                                #     othewrise wait until dependency step completed.
     "trigger_start" : False,   # --> wait until receive a trigger messgage to run.                          
     "name"          : "task1", # --> current step name, also the function name of the step 
                                      implementation.
     "mproc_num"     : 1        # --> number of processes 
 },
 {
     "dependency"    : "task1", # --> dependency workflow step
     "join_wait"     : False,   # --> if False, the step will start immediately, 
                                # --> othewrise wait until dependency workflow step completed.
     "trigger_start" : False,                              
     "name"          : "task2", # --> current step name, the logic is defined in a function, 
                                # --> which is invoked by the framework 
     "mproc_num"     : 8        # --> number of processes 
 } 
 
#---------------------------------------------------------------------------------#

#--- actual code to implement step : task1
def task1(param, task=None, q_log=None, q_out=None) :
  for _ in range(20000) : 
    mp.push_q(q_out, 'task')
  return param


#--- sample code to implement step : task2

def task2(param, task=None, q_log=None, q_out=None) :
  if param['last_call'] == False :
    time.sleep (random.uniform(0, 0.1))
    for _ in range( int(40 * random.uniform(0, 1))) : 
      mp.push_q(q_out, 'task')  
  else :
    tb.write_file ('R:/aa.log', 'done')
  param['last_call_required'] = True    
  return param 
 

#==================================================================================
```


Please refer to test/test.py for a complete example.

##### Screenshot:

![Screenshot](https://github.com/gr82morozr/py3mproc/blob/master/docs/windows10.png)



To install :

- pip install py3mproc

For upgrade:

- pip install py3mproc --upgrade -vvv  --no-cache-dir




