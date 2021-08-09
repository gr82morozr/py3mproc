#!/usr/bin/python3
"""
 Multi-Processing Framework 
  * A general python3 workflow execution framework
  * support boss worker model (1:M)
  * support assembly line model (1:1)
  * support simple map reduce model (M:1)
  
  
 To-do:
  ? jumping worklflow step 
  ? dynamic routing
  ? HTML (javascript) based monitor
  ? support external messaging system to overcome python FIFO queue issue.
  
"""

import os,sys,time,multiprocessing
import psutil
import json
import py3toolbox as tb
import time
import random

def default_config() :
  config = {
            "default" : {
                "start_task"        : "__START_TASK__",
                "end_task"          : "__END_TASK__",
                "kill_task"         : "__KILL_TASK__",    
                "id_template"       : "#ID#",    
                "end_task_max"      : 4,
                "q_timeout"         : 120,
                "q_throttled_max"   : 9990,
                "end_task_qmin"     : 100,
                "max_errors"        : 0,
                "progress_bar_max"  : 80,
                "proc_status"       : {
                                        "null"            : " ",
                                        "started"         : "↑",
                                        "waiting"         : "_",
                                        "running"         : "►",
                                        "done"            : "√",
                                        "ended"           : "○",
                                        "exited"          : ".",                      
                                        "error"           : "!",
                                        "killed"          : "X"
                                      },
                "log_format"        : "{0:19} : {1:16} :{2:8}: {3:5} - {4}",
                "log_interval"      : 120,
                "log_batch"         : 200,
                "log_line_max"      : 100000,
                "log_levels"        : {
                                        "ERROR"           : 50,
                                        "INFO"            : 40,
                                        "DEBUG"           : 10
                                      }
             }
        }
  return config

"""
 -------------------------------------------------------------
 -  General Shared Function
 -   * can be called any where in the module or even external.
 -------------------------------------------------------------
"""

def gen_log(comp=None, task_uid=None, loglvl=None, logtxt=None, q_log=None, log_filter=None) :
  
  if comp is None     :  comp       = '........'
  if loglvl is None   :  loglvl     = 'DEBUG'
  if task_uid is None :  task_uid   = '--------'
  
  if log_filter == 'ERROR' and (loglvl == 'DEBUG' or loglvl == 'INFO') :  return
  if log_filter == 'INFO'  and loglvl == 'DEBUG'  :  return  
  
  log = {}
  log['timestamp'] = tb.get_timestamp(iso_format=True)
  log['component'] = comp
  log['task_uid']  = task_uid
  log['log_level'] = loglvl
  log['log_text']  = logtxt
  q_log.put(log)




def set_proc_priority(level, pid=None):
    """Set the priority/nice of the application.
    
    Numbers may be used (in the style of Linux from -20 (high) to 19 (low),
    or as text, such as 'belownormal' or 'realtime'.
    """
    process = psutil.Process(pid)
    try:
        level = level.lower().replace(' ', '')
        
        if level == 'realtime':
            process.nice(psutil.REALTIME_PRIORITY_CLASS)
        elif level == 'high':
            process.nice(psutil.HIGH_PRIORITY_CLASS)
        elif level == 'abovenormal':
            process.nice(psutil.ABOVE_NORMAL_PRIORITY_CLASS)
        elif level == 'normal':
            process.nice(psutil.NORMAL_PRIORITY_CLASS)
        elif level == 'belownormal':
            process.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
        if level == 'low':
            process.nice(psutil.IDLE_PRIORITY_CLASS)
            
    except AttributeError:
        if level < -13:
            process.nice(psutil.REALTIME_PRIORITY_CLASS)
        elif -13 <= level < -7:
            process.nice(psutil.HIGH_PRIORITY_CLASS)
        elif -7 <= level < 0:
            process.nice(psutil.ABOVE_NORMAL_PRIORITY_CLASS)
        elif 0 <= level < 7:
            process.nice(psutil.NORMAL_PRIORITY_CLASS)
        elif 7 <= level < 12:
            process.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
        elif 13 <= level:
            process.nice(psutil.IDLE_PRIORITY_CLASS) 



"""
 -------------------------------------------------------------
 -  Worker
 -    * main worker process
 -    * take task messange to input queue and execute it
 -    * once complete, generate task 
 -    * pass next task to next workflow step
 -------------------------------------------------------------
"""
class Worker(multiprocessing.Process):
  def __init__(self, config, idx, q_log , q_mgr, name):
    multiprocessing.Process.__init__(self)  
    self.config         = config[name]
    self.name           = self.config['name']
    self.id             = idx
    self.u_name         = self.name + '_' + str(self.id)
    self.task_uid       = tb.get_uid()
    self.q_in           = self.config['q_in']
    self.q_out          = self.config['q_out']
    self.q_timeout      = self.config['q_timeout']
    self.q_log          = q_log
    self.q_mgr          = q_mgr
    self.task           = None
    self.task_status    = {}
    task_module         = __import__(self.config['task_module'])    
    self.exec_task      = eval('task_module.' + self.config['name'])
    self.start_task     = self.config['start_task'] + self.config['id_template'].replace('ID',str(self.id))    
    self.end_task       = self.config['end_task']   + self.config['id_template'].replace('ID',str(self.id))
    self.trigger_start  = self.config['trigger_start']
    self.max_errors     = self.config['max_errors']
    self.end_task_max   = self.config['end_task_max']
    self.end_task_qmin  = self.config['end_task_qmin']
    self.log_filter     = self.config['log_level']
    self.errors_count   = 0
    self.tasks_count    = 0
    self.own_end_rcvd   = 0
    self.end_task_dic   = {}
    
    self.param          = {
                            'name'               : self.name,
                            'u_name'             : self.u_name, 
                            'task_uid'           : self.task_uid,
                            'last_call_required' : False, 
                            'last_call'          : False,
                            'log_task_func'      : self.log_task,
                            'output_task_func'   : self.output_task,
                            'task'               : None,
                            'config'             : config
                          }
    self.task_exec_time = 0
    
 
    
  def get_task(self):
    task = None
    while True:
      try :  
        self.task_uid = tb.get_uid()
        self.update_status('waiting')
        task = self.q_in.get(timeout=self.q_timeout)
        if task is not None:
          self.task = task
          break
        else:
          self.log_task(loglvl = 'DEBUG', logtxt = 'task waiting timeout :' + str(self.q_timeout))
      except :
        pass    

  def execute_task(self) :
    self.param['task_uid'] = self.task_uid
    if self.param['last_call'] == False: self.tasks_count +=1
    self.update_status('running')

    self.task_exec_time = tb.timer_start()
    self.param['task'] = self.task
    # invoke external function to implement the task
    #self.log_task(loglvl = 'DEBUG', logtxt = 'task_param={' +  str(self.param)  + '}')
    self.param = self.exec_task(param=self.param)
    self.task_exec_time = int(tb.timer_check(self.task_exec_time) * 1000)/1000
    self.update_status('done')
    pass 
 
  def update_status(self, status):
    self.task_status =  { 
                          'name'          : self.name, 
                          'id'            : self.id , 
                          'status'        : status, 
                          'tasks_count'   : self.tasks_count,
                          'errors_count'  : self.errors_count
                        }
    if status == 'running' :
      self.log_task(loglvl = 'INFO', logtxt = status + ' - #' + str(self.tasks_count) + ' task is runing now.')
    elif  status == 'done' :
      self.log_task(loglvl = 'INFO', logtxt = status + ' - #' + str(self.tasks_count) + ' task is completed, exec_time = ' + str(self.task_exec_time) + ' s')
    elif  status == 'waiting' :
      self.log_task(loglvl = 'INFO', logtxt = status + ' for next task ... ')
    else :
      self.log_task(loglvl = 'DEBUG',logtxt = status + ' - #' + str(self.tasks_count) + ', task status = ' + status)
    
    self.q_mgr.put(self.task_status)
    """
    if self.q_mgr.qsize() > 1000 : 
      if (status in [ "exited" , "running", "done", "started", "ended", "error" ] ) or (self.tasks_count % 20 == 0):
        self.q_mgr.put(self.task_status)
    else:
      self.q_mgr.put(self.task_status)
    """

  def output_task(self, message=None):
    if message is None : return
    config = default_config()
    q_throttled_max = config['default']['q_throttled_max']
    while True:
      if self.q_out.qsize() <= q_throttled_max :
        self.q_out.put(message)
        break
      else:
        time.sleep(1)
    
    
  def log_task(self, loglvl, logtxt):
    # wrapper log function for logging stpes inside task.
    gen_log(comp = self.u_name, task_uid = self.task_uid , loglvl = loglvl, logtxt = logtxt , q_log=self.q_log, log_filter=self.log_filter)
  
  def run(self):
    self.log_task(loglvl = 'DEBUG', logtxt = self.u_name + ' started, pid=' + str(self.pid))
    self.update_status('started')
    time.sleep(2)
    while True: 
      self.task           = None
      try:
        if self.q_in is not None:
          self.get_task()
          self.log_task(loglvl = 'DEBUG', logtxt = 'new_task = ' +  str(self.task)  )

          # check trigger start run once
          if self.trigger_start == True and self.task == self.start_task :
            self.own_end_rcvd = 0
            self.end_task_dic = {}
            self.execute_task()
            self.log_task(loglvl = 'INFO', logtxt = 'trigger-start : execution completed')
            self.update_status('ended')
            break
          
          # exception, there should be no self.start_task coming in
          if self.trigger_start == False and self.task == self.start_task :
            self.log_task(loglvl = 'ERROR', logtxt = 'There should be no self.start_task coming in', q_log=self.q_log, log_filter=self.log_filter)
            continue
            
          # normal task coming in(looping to consume more)
          if self.config['end_task'] not in self.task :
            self.own_end_rcvd = 0
            self.end_task_dic = {}
            self.execute_task()
            continue

          # other workers end task coming in, the pass on
          # if self.config['end_task'] in self.task and self.task != self.end_task :
          if self.config['end_task'] in self.task :
            if self.task in self.end_task_dic and self.q_in.qsize() <= self.end_task_qmin :
              self.end_task_dic[self.task] +=1
            else:
              self.end_task_dic[self.task] =1
            
            if self.end_task_dic[self.task] < self.end_task_max :  
              # if receives other workers end_task, and NOT exceeds limit, pass on
              time.sleep(random.uniform(0, 1))
              self.q_in.put(self.task)
              time.sleep(random.uniform(0, 1))
              self.log_task(loglvl = 'INFO', logtxt = 'end_task received (' + str(self.task) + '), passing on ... ')
              continue
            else : 
            # if receives other workers end_task, if reached limit, focrce end this worker
            
              # last call has no 'task' passed in, just execute to close off remaining tasks
              if self.param['last_call_required'] == True:
                self.param['last_call'] = True
                self.log_task(loglvl = 'DEBUG', logtxt = 'last_call task to be executed ...')
                self.execute_task()  
                self.log_task(loglvl = 'INFO',  logtxt = 'last_call task completed')
              
              # if no last call required, 
              self.log_task(loglvl = 'DEBUG', logtxt = 'end_task : ' + str(self.end_task_dic[self.task])+ '/' + str(self.end_task_max) + ' : to be ended')
              self.update_status('ended')
              break

        else :
        # No start trigger required, start task immediately
          self.log_task(loglvl = 'INFO', logtxt = 'trigger-start : n/a, starting immediately now')
          self.execute_task()
          self.update_status('ended')
          break
      except KeyboardInterrupt:
        self.log_task(loglvl = 'INFO', logtxt = self.u_name + ': CTL-C Interrupted!')
        self.q_mgr.put(self.config['kill_task'])
        break          
      except Exception as err: 
        self.errors_count +=1
        self.update_status('error')
        self.log_task(loglvl = 'ERROR', logtxt = 'task={' +  str(self.task)  + '}')
        self.log_task(loglvl = 'ERROR', logtxt = 'Error : ' + str(err) )
        if  self.trigger_start == False and self.task is not None: 
          # pass task to other works to retry
          self.q_in.put(self.task)
        else:
          self.update_status('killed')
          break
        
          
"""
 -------------------------------------------------------------
 -  Monitor 
 -   * monitor entire workflow execution in realtime
 -   * render monitor screen
 -------------------------------------------------------------
"""

class Monitor(multiprocessing.Process):        
  def __init__(self, config, q_mtr , q_log,  q_mgr  ):    
    multiprocessing.Process.__init__(self)  
    self.config         = config
    self.q_mtr          = q_mtr
    self.q_mgr          = q_mgr
    self.q_log          = q_log
    self.u_name         = 'Monitor'
    self.task_uid       = '00000001'
    self.end_task_max   = self.config['default']['end_task_max']
    self.end_task_count = 0
    self.q_timeout      = 1
    self.log_filter     = self.config['logger']['log_level']
    self.wf_status      = None
    self.last_wf_status = None
    self.start_time     = tb.timer_start()
    self.max_workers    = None      
    self.top_banner     = ''
    self.bottom_banner  = ''
    self.empty_title    = ''
    self.format_str     = ''
    self.init_monitor()
    pass

  def init_monitor(self):
  
    self.top_banner   = '        {0:<20}'.format(tb.render('[%BRIGHT|MAGENTA_BG:  ' + self.config['title'] + '  %]', align='<', width =15) )
    bottom_banner_str = ' {0:>10} : {1:1} {2:>10} : {3:1} {4:>10} : {5:1} {6:>10} : {7:1}'
    
    self.bottom_banner += bottom_banner_str.format(
      tb.render('[%BLUE_BG|LYELLOW: STARTED %]', align='>', width=15) , tb.render('[%LCYAN:' + self.config ['default']['proc_status']['started'] + '%]', align='<', width=1),
      tb.render('[%BLUE_BG|LYELLOW: WAITING %]', align='>', width=15) , tb.render('[%LCYAN:' + self.config ['default']['proc_status']['waiting'] + '%]', align='<', width=1),
      tb.render('[%BLUE_BG|LYELLOW: RUNNING %]', align='>', width=15) , tb.render('[%LCYAN:' + self.config ['default']['proc_status']['running'] + '%]', align='<', width=1),
      tb.render('[%BLUE_BG|LYELLOW:  DONE   %]', align='>', width=15) , tb.render('[%LCYAN:' + self.config ['default']['proc_status']['done'] + '%]', align='<', width=1)) + '\n'
    self.bottom_banner += bottom_banner_str.format(
      tb.render('[%BLUE_BG|LYELLOW:  ENDED  %]', align='>', width=15) , tb.render('[%LCYAN:' + self.config ['default']['proc_status']['ended'] + '%]', align='<', width=1),
      tb.render('[%BLUE_BG|LYELLOW: EXITED  %]', align='>', width=15) , tb.render('[%LCYAN:' + self.config ['default']['proc_status']['exited'] + '%]', align='<', width=1),
      tb.render('[%BLUE_BG|LYELLOW:  ERROR  %]', align='>', width=15) , tb.render('[%LCYAN:' + self.config ['default']['proc_status']['error'] + '%]', align='<', width=1),
      tb.render('[%BLUE_BG|LYELLOW: KILLED  %]', align='>', width=15) , tb.render('[%LCYAN:' + self.config ['default']['proc_status']['killed'] + '%]', align='<', width=1))+ '\n'

  def map_status (self, status):
    mapped_status = list (status)
    for i in range(len(mapped_status)) :
      mapped_status[i] = self.config ['default']['proc_status'][mapped_status[i]]
    return "".join(mapped_status)

  def get_message(self):
    try :  
      message = self.q_mtr.get(timeout=self.q_timeout)
      return message
    except : 
      return None

  def get_qsize(self, q_step):
    for step in self.config['workflow']['steps']:
      name  = step['name']
      if q_step == name : 
        if step['q_in'] is None : return 0
        return step['q_in'].qsize()
    return 0

  def refresh (self,actual_interval=1):
    self.format_str     = '  {0:>4} : {1:<12} : {2:>8} → [{3:<' + str(self.max_workers) + '}] → {4:>8}|{5:>5}  {6:>6}|{7:>5}   '
    output              = []
    output_str          = ''
    self.empty_title = self.format_str.format('','','','','','','', '')
    l_width = len(self.empty_title)
    l_title = len(self.config['title'])
    
    # Avoid divided by 0 error
    if actual_interval == 0 : actual_interval = 1
    
    self.top_banner =  tb.render256('[%B|FG226|BG127|C|' + str(l_width) + ':' + self.config['title'] + '%]')
    
    header = self.format_str.format(
      '#Seq', 
      tb.render('[%LGREEN:Step%]',align='<',width=12), 
      tb.render('[%LRED:Pending%]',align='>',width=8), 
      tb.render256('[%B|FG087|BG235|C|' + str(self.max_workers) + ':Workers%]'),
      tb.render('[%LGREEN:Done%]',align='>',width=8), 
      tb.render('[%RED:Error%]',align='>',width=5), 
      'Rate', 
      'Avg/s'
    )
	
    output.append ("")
    output.append (self.top_banner)
    #output.append (tb.render('[%LYELLOW:' + '=' * len(self.empty_title) + '%]'))
    output.append ("")
    output.append (header)
    output.append (tb.render('[%LBLUE:' + '-' * len(self.empty_title) + '%]'))  
	
    # Keep track of last status, then calculate summary
    if self.last_wf_status is None :
      self.last_wf_status = self.wf_status


    for idx in range(len(self.wf_status.keys())):  
      for step in  self.wf_status.keys() :
        if self.wf_status[step]['seq'] == (idx + 1) :
          rate_now = int((self.wf_status[step]['tasks_count_sum'] - self.last_wf_status[step]['tasks_count_sum'])/actual_interval)
          rate_avg = int((self.wf_status[step]['tasks_count_sum'] / tb.timer_check(self.start_time)))
          _workers_status = self.map_status(self.wf_status[step]['workers'])
          _last_workers_status = self.map_status(self.last_wf_status[step]['workers'])
          _seq     = self.wf_status[step]['seq']
          # change color when all worker processes completed.
          # - prevent incorrect FIFO sequence
          if _last_workers_status == '.' * len(_workers_status): _workers_status = _last_workers_status
          if _workers_status != '.' * len(_workers_status) :
            _step   =  tb.render('[%LGREEN|BRIGHT:'  + step + '%]',align='<'  ,width=12)
            _workers = tb.render256('[%B|FG087|BG235|L|' + str(self.max_workers) + ':' + _workers_status + '%]')
            
          else:
            _step   =  tb.render('[%DGRAY:'     + step + '%]',align='<'  ,width=12)
            _workers = tb.render256('[%B|FG087|BG235|L|' + str(self.max_workers) + ':' + _workers_status + '%]')
            
          
          _pending = tb.render('[%LRED:'      + str(self.get_qsize(step))                       + '%]',align='>'  ,width=8)
          _done    = tb.render('[%LGREEN:'    + str(self.wf_status[step]['tasks_count_sum'])    + '%]',align='>'  ,width=8)
          _error   = tb.render('[%RED:'       + str(self.wf_status[step]['errors_count_sum'])    + '%]',align='>' ,width=5)
          
          if self.wf_status[step]['errors_count_sum'] > 99999:
            _error   = tb.render('[%RED:  MAX%]',align='>' ,width=5)
          
          output.append (self.format_str.format(_seq, _step ,_pending, _workers, _done , _error,  rate_now, rate_avg))
          
          break
          
          
    output.append (tb.render('[%LBLUE:' + '-' * len(self.empty_title) + '%]'))     
    output.append( '  {0:>4} - {1:<12} : {2:>11}'.format (' ', tb.render('[%LYELLOW:Logger%]', align='<',width=12), tb.render('[%LCYAN:' + str(self.q_log.qsize()) + '%]', align='>', width=11)  ))
    output.append( '  {0:>4} - {1:<12} : {2:>11}'.format (' ', tb.render('[%LYELLOW:Monitor%]',align='<',width=12), tb.render('[%LCYAN:' + str(self.q_mtr.qsize()) + '%]', align='>', width=11)  ))
    output.append( '  {0:>4} - {1:<12} : {2:>11}'.format (' ', tb.render('[%LYELLOW:Manager%]',align='<',width=12), tb.render('[%LCYAN:' + str(self.q_mgr.qsize()) + '%]', align='>', width=11)  ))
    output.append(tb.render('[%LBLUE:' + '-' * len(self.empty_title) + '%]'))
    output.append(self.bottom_banner)
 
    bottom_title=  tb.render256('[%B|FG226|BG127|C|' + str(l_width) + ':' + tb.get_timestamp() + ' - ' +  'Time elapsed : ' + str( int (tb.timer_check(self.start_time) )) + ' s%]')
    
    output.append (bottom_title) 
    output_str = '\n'.join(output)
    
    
    if self.config ['monitor']['refresh'] == True:  tb.cls()
    print (output_str)
      
  def get_max_workers(self):
    max_workers = 10
    for idx in range(len(self.wf_status.keys())):  
      for step in  self.wf_status.keys() :
        if max_workers < len(self.wf_status[step]['workers']):
          max_workers = len(self.wf_status[step]['workers'])
    self.max_workers = max_workers
    return
    
    
    
  def run(self): 
    print ("Monitor started : pid = " + str(self.pid))
    now = tb.timer_start()
    while True:
      try :
        # read message
        message = self.get_message()
        
        # if no message, continue
        if message is None : 
          time.sleep(0.1)
          continue
        
        # if message is __END_TASK__
        if message  == self.config['default']['end_task']:  
          # Monitor end task command coming in
          self.end_task_count +=1
          self.refresh(actual_interval=tb.timer_check(now))
          if self.end_task_count >= self.end_task_max:
            break
          else:
            self.q_mtr.put(self.config['default']['end_task'])
            time.sleep(random.uniform(0, 1))
            continue
        
        else:
          
          # regular task coming in
          end_task_count  = 0
          self.wf_status = message
          
          if (tb.timer_check(now)) >= self.config ['monitor']['refresh_interval']  : 
            # now it's time to refresh
            if self.wf_status is None: continue
            if self.max_workers is None: self.get_max_workers()
            # refresh the screen
            self.refresh(actual_interval=tb.timer_check(now))
            self.last_wf_status = self.wf_status
            now = tb.timer_start()
            
      except KeyboardInterrupt:
        gen_log(comp = self.u_name, task_uid = self.task_uid , loglvl = 'INFO', logtxt = self.u_name  +  ': CTL-C Interrupted!', q_log=self.q_log, log_filter=self.log_filter)
        self.q_mgr.put(self.config ['default']['kill_task'])
        
      except Exception as err:
        gen_log(comp = self.u_name, task_uid = self.task_uid , loglvl = 'ERROR', logtxt = err, q_log=self.q_log, log_filter=self.log_filter)
    pass

"""
 -------------------------------------------------------------
 -  Logger
 -    * log messages received from other processes
 -    * write into disk by batch to improve performance
 -    * split log file into multiple for fast query
 -------------------------------------------------------------
"""

class Logger(multiprocessing.Process):        
  def __init__(self, config, q_mtr , q_log,  q_mgr  ):    
    multiprocessing.Process.__init__(self)  
    self.config         = config
    self.u_name         = 'Logger'
    self.task_uid       = '00000002'
    self.q_mtr          = q_mtr
    self.q_log          = q_log
    self.q_mgr          = q_mgr
    self.q_timeout      = self.config['default']['q_timeout']
    self.end_task       = self.config['default']['end_task']
    self.end_task_max   = self.config['default']['end_task_max']
    self.own_end_rcvd   = 0
    self.log_file       = self.config['logger']['log_file']
    self.log_file_count = 0
    self.log_line_count = 0
    self.log_time       = tb.timer_start()
    self.log_interval   = self.config['default']['log_interval']
    self.log_line_max   = self.config['default']['log_line_max']
    self.log_level      = self.config['default']['log_levels'][self.config['logger']['log_level']]
    self.log_format     = self.config['default']['log_format']
    self.log_batch      = self.config['default']['log_batch']
    self.log_messages   = []
    self.init_log_file()

  def init_log_file(self):
    log_folder = tb.get_file_folder(self.log_file)
    tb.mk_folder(log_folder)
    for f in tb.gen_files(folder = tb.get_file_folder(self.log_file), type='file'):
      if tb.get_file_name(self.log_file) in f:
        tb.rm_file (f)
    
  def get_message(self):
    while True:
      try :  
        message = self.q_log.get(timeout=self.q_timeout)
        return message
      except : 
        pass
     
  def flush_log(self):
    log_formatted = ''
    self.log_time = tb.timer_start()
    
    for log_message in self.log_messages:
      timestamp     = log_message['timestamp']
      component     = log_message['component']
      task_uid      = log_message['task_uid']
      log_text      = log_message['log_text']

      self.log_line_count +=1 
      log_formatted = log_formatted + tb.format_str(self.log_format, timestamp, component, task_uid, log_message['log_level'], log_text ) + '\n'
      
      # add empty line
      if (self.log_line_count + 1) % self.log_batch == 0 : 
        log_formatted = log_formatted + "\n"
        self.log_line_count +=1
    
      # write to new file and finish current log file
      if self.log_line_count >= self.log_line_max:
        tb.write_log(file_name=self.log_file + '.' + str(self.log_file_count) , text=log_formatted)
        log_formatted = ''
        self.log_line_count = 0
        self.log_file_count +=1
    
    if log_formatted != '' :
      log_formatted = log_formatted[:-1] # rstrip last new line
      tb.write_log(file_name=self.log_file + '.' + str(self.log_file_count) , text=log_formatted)
    
    self.log_messages = []
    return
  
  def run(self): 
    print ("Logger started : pid = " + str(self.pid))
    while True:
      try:
        message = self.get_message()
        
        # normal log msg coming in(looping to consume more)
        if message != self.end_task :
          self.own_end_rcvd = 0
          if 'log_level' in message :
            if self.config['default']['log_levels'][message['log_level']] < self.log_level : 
              continue      
          self.log_messages.append(message)
          
        # When buffer full, write log immediately 
        if len(self.log_messages) >= self.log_batch - 1:
          self.flush_log()    
        
        # When error, write log immediately 
        if 'log_level' in message :
          if message['log_level'] == 'ERROR':
            self.flush_log() 
          
        # log every (log_interval) seconds
        if (tb.timer_check(self.log_time)  > self.log_interval) :
          self.flush_log()


        # in case of timeout, message = None
        if message is None:
          gen_log(comp = self.u_name, task_uid = self.task_uid , loglvl = 'DEBUG', logtxt = 'Log message timeout: ' + str(self.q_timeout), q_log=self.q_log)
          self.flush_log()
               
          
        # own end task coming in - stopping the logger            
        if message == self.end_task and self.own_end_rcvd < self.end_task_max  : 
          self.flush_log() 
          self.own_end_rcvd +=1
          self.q_log.put(self.end_task)
          

        # own end task coming in and reached max limit - stop the logger now
        if message == self.end_task and self.own_end_rcvd >= self.end_task_max  : 
          self.flush_log()    
          break   

        
      except KeyboardInterrupt:
        gen_log(comp = self.u_name, task_uid = self.task_uid , loglvl = 'INFO', logtxt = self.u_name + ': CTL-C Interrupted!', q_log=self.q_log)
        self.q_mgr.put(self.config['default']['kill_task'])
        break         
    pass    


"""
 -------------------------------------------------------------
 -  Manager
 -    * manage the entire workflow execution
 -    * check individual workflow step
 -    * send control signal of workflow steps (worker)
 - 
 -------------------------------------------------------------
"""

class Manager(multiprocessing.Process):
  def __init__(self, wf_json=None, wf_config=None): 
    multiprocessing.Process.__init__(self)
    
    self.wf_config              = default_config()
    
    if wf_json is not None      :   self.wf_config.update(tb.load_json(wf_json))
    elif wf_config is not None  :   self.wf_config.update(wf_config)
    else :                          raise ValueError('Missing parameters for Manager()')

    self.u_name                 = 'Manager'
    self.task_uid               = '00000000'
    self.q_mgr                  = multiprocessing.Queue()
    self.q_log                  = multiprocessing.Queue()  
    self.q_mtr                  = multiprocessing.Queue() 
    self.q_timeout              = self.wf_config['default']['q_timeout']
    self.log_filter             = self.wf_config['logger']['log_level']
    self.message                = None
    self.worker_config            = {}
    self.workers                = {}
    self.wf_status              = {}
    self.wf_ended               = False

    return
    
    
  def get_message(self):
    try :  
      message = self.q_mgr.get(timeout=self.q_timeout)
      return message
    except : 
      return None

  def send_task_control_signal(self, q, number, signal):
    for _ in range(number): q.put(signal)

  def update_wf_status(self, message=None):
    index = 0
    
    if message is None:  
      # init workflow status
      for step in self.wf_config['workflow']['steps']:
        index +=1
        name = step['name']
        self.wf_status[name] = {}
        self.wf_status[name]['seq']               = index
        self.wf_status[name]['workers']           = ['null'] * self.worker_config[name]['mproc_num']
        self.wf_status[name]['tasks_count']       = [0] * self.worker_config[name]['mproc_num']
        self.wf_status[name]['errors_count']      = [0] * self.worker_config[name]['mproc_num']
        self.wf_status[name]['tasks_count_sum']   = sum(self.wf_status[name]['tasks_count'])
        self.wf_status[name]['errors_count_sum']  = sum(self.wf_status[name]['errors_count'])
    else :
      # update workflow status
      name = message['name']
      id   = message['id']
      self.wf_status[name]['workers'][id]         = message['status']
      self.wf_status[name]['tasks_count'][id]     = message['tasks_count']
      self.wf_status[name]['errors_count'][id]    = message['errors_count']
      self.wf_status[name]['tasks_count_sum']     = sum(self.wf_status[name]['tasks_count'])
      self.wf_status[name]['errors_count_sum']    = sum(self.wf_status[name]['errors_count'])
      
    return
  
  def kill_proc(self, pid):
    p = psutil.Process(pid)
    p.kill()  

  def clean_procs(self, step_name=None, terminate=True, join=True):
    if step_name is None:
      for step in self.wf_config['workflow']['steps']:
        name  = step['name']
        for w in self.workers[name] : 
          gen_log(comp = self.u_name, task_uid = self.task_uid , loglvl = 'INFO', log_filter= self.log_filter, logtxt = 'Teminating processes [' + name + ' | pid=' + str(w.pid) + '] ... ', q_log=self.q_log) 
          if psutil.pid_exists(w.pid):
            if terminate:  self.kill_proc(w.pid)
            if join : w.join()

	    # turn off Logger and Monitor processes
      gen_log(comp = self.u_name, task_uid = self.task_uid , loglvl = 'INFO',  log_filter= self.log_filter, logtxt = 'Stopping Monitor and Logger', q_log=self.q_log) 
      self.q_mtr.put (self.wf_config['default']['end_task'])    
      self.monitor.join()      
      time.sleep(10)

      self.q_log.put (self.wf_config['default']['end_task'])
      self.logger.join()
      time.sleep(10)


    else:
      name  = step_name
      gen_log(comp = self.u_name, task_uid = self.task_uid , loglvl = 'INFO',  log_filter= self.log_filter, logtxt = 'Teminating processes [' + name + '] - ' + str(len(self.workers[name])) + ' ... ', q_log=self.q_log)  
      for w in self.workers[name] : 
        if psutil.pid_exists(w.pid):
          if terminate: self.kill_proc(w.pid)
          if join : w.join()
      
    return 


  def wait_proc_start(self, proc, name):
    while True:
      time.sleep(0.2)
      if proc.pid is not None:
        message =  '[' +  name + '] pid=' + str(proc.pid)
        if name != 'Logger' :
          gen_log(comp =self.u_name, task_uid=self.task_uid, loglvl = 'DEBUG', logtxt = message, q_log=self.q_log)
        break;
    return proc.pid 
  
  def start_wf(self):
	# ===================================
	# kick off workflow
	# ===================================
	
    # setup q_out and others
    for step in self.wf_config['workflow']['steps']:
      name = step['name']
      self.worker_config[name] = step
      self.worker_config[name]['q_out'] = multiprocessing.Queue()
      self.worker_config[name]['task_module'] = self.wf_config['task_module']
      self.worker_config[name].update(self.wf_config['default'])
      self.worker_config[name].update(self.wf_config['logger'])
      self.worker_config[name].update(self.wf_config['monitor'])
    
    # setup q_in, connect all steps by queues
    for step in self.wf_config['workflow']['steps']:
      name  = step['name']
      dependency_name = step["dependency"]
      if len(step["dependency"]) > 0:
        self.worker_config[name]['q_in'] = self.worker_config[dependency_name]['q_out']
      else :
        self.worker_config[name]['q_in'] = None

    # start Logger & Monitor
    self.logger       = Logger (config = self.wf_config, q_mtr = self.q_mtr, q_log = self.q_log, q_mgr = self.q_mgr)
    self.monitor      = Monitor(config = self.wf_config, q_mtr = self.q_mtr, q_log = self.q_log, q_mgr = self.q_mgr)
    
    self.logger.start()
    self.wait_proc_start(self.logger, 'Logger')

    
    self.monitor.start()
    monitor_pid = self.wait_proc_start(self.monitor, 'Monitor')
    set_proc_priority("high",monitor_pid)
    
    
    time.sleep(2)
    self.update_wf_status()
    
    
    # start workers
    for step in self.wf_config['workflow']['steps']:
      name  = step['name']
      dependency_name = step["dependency"]
      if step['join_wait'] == False:    
        self.workers[name] = [ Worker(config=self.worker_config, idx = i, q_log = self.q_log, q_mgr = self.q_mgr, name=name ) for i in range(self.worker_config[name]['mproc_num']) ]
        #self.workers[name] = [ Worker(config=self.worker_config, idx = i, q_log = self.q_log, q_mgr = self.q_mgr ) for i in range(self.worker_config[name]['mproc_num']) ]
        for w in self.workers[name] : 
          w.start()
          self.wait_proc_start(w, name)
        time.sleep(2)
      
    return

  def set_step_exited(self, step_ended):
    # set wf step to be 'exited'
    index = 0
    seq = self.wf_status[step_ended]['seq']
    for step in self.wf_config['workflow']['steps']:
      index +=1
      step_name = step['name']
      if index <= seq :
        self.wf_status[step_name]['workers'] = ['exited'] * len(self.wf_status[step_name]['workers'])    
      else:
        break

  def is_wf_ended(self, step_ended):
    is_wf_ended  = True
    
    for step in self.wf_status.keys() :
      if set(self.wf_status[step]['workers']) != {'exited'} : is_wf_ended = False 
    self.wf_ended = is_wf_ended
    return self.wf_ended 

  def run_next_step(self, step_ended):
    # run next step
    for step in self.wf_config['workflow']['steps']:
      name  = step['name']
      dependency_name = step['dependency']
      if dependency_name != step_ended : continue
      
      # start listening for "join_wait=True" step
      if step['join_wait'] == True : 
        self.workers[name] = [ Worker(config=self.worker_config, idx = i, q_log = self.q_log, q_mgr = self.q_mgr, name=name) for i in range(self.worker_config[name]['mproc_num']) ]
        for w in self.workers[name] : 
          w.start()
          self.wait_proc_start(w, name)     
      
      # send start signal, this is specifically for task , who doesn't need prev task output as it's input, 
      # - only requires a singal to start   
      if step['trigger_start'] == True :  
        for id in range(self.worker_config[name]['mproc_num']):
          start_signal = self.wf_config['default']['start_task'] +  self.wf_config['default']['id_template'].replace('ID',str(id))
          self.send_task_control_signal(q = self.worker_config[name]['q_in'], number = 1, signal = start_signal)
      
      # Always send end signal, it should be at the LAST of the queues. ( however Python has FIFO issue for queues)
      for id in range(self.worker_config[name]['mproc_num']):
        end_signal = self.wf_config['default']['end_task'] +  self.wf_config['default']['id_template'].replace('ID',str(id))
        self.send_task_control_signal(q = self.worker_config[name]['q_in'], number = 1, signal = end_signal)

    pass

  def move_wf(self) :
    for step_name in self.wf_status.keys():
      if set(self.wf_status[step_name]['workers']) == {'ended'}:
        self.set_step_exited(step_name)
        if self.is_wf_ended (step_name) == False:   
          self.run_next_step(step_name)


  def run_wf_wrapper(self): 
    # start all processes (except join_wait=True)
    self.start_wf()
    while True:  
      self.message = self.get_message()
      if self.message is None : continue
      if self.message ==  self.wf_config['default']['kill_task'] :
        self.clean_procs()
        exit(1)
      self.update_wf_status(self.message)
      self.move_wf()
      self.q_mtr.put(self.wf_status)
      if self.wf_ended == True:  
        self.close_wf()
        break

  def close_wf (self):
    self.clean_procs()
    gen_log(comp = self.u_name, task_uid = self.task_uid , loglvl = 'INFO', logtxt = 'All completed', q_log=self.q_log)

  def run(self): 
    tb.cls()
    try :
      self.run_wf_wrapper()
      return
    except KeyboardInterrupt:
      print(self.u_name  +  ' : CTL-C Interrupted!')   
      self.clean_procs() 
      return
