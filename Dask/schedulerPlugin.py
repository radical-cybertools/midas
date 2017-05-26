from distributed.diagnostics.plugin import SchedulerPlugin

"""
When the plugins is to be used the the following should be done:

c=Client($SCHEDULER)

c.run_on_scheduler(submitCustomProfiler,'Profile.txt')

after the end of the execution

do

c.run_on_scheduler(removeCustomProfiler)

"""

def submitCustomProfiler(profname,dask_scheduler):
    prof = MyProfiler(profname)
    dask_scheduler.add_plugin(prof)

def removeCustomProfiler(dask_scheduler):
    dask_scheduler.remove_plugin(dask_scheduler.plugins[-1])

class MyProfiler(SchedulerPlugin):
    def __init__(self,profname):
        self.profile = profname

    def transition(self,key,start,finish,*args,**kargs):
        if start == 'processing' and finish == 'memory':
            with open(self.profile,'a') as ProFile:
                ProFile.write('{}\n'.format([key,start,finish,kargs['worker'],kargs['thread'],kargs['startstops']]))

