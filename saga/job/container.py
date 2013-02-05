
__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"


from saga.task    import Container as TaskContainer
from saga.job.job import Job


# 'forward' declaration of job.Container
class Container (TaskContainer):
    """ :todo: document me

        .. py:attribute:: jobs

           The (read-only) `jobs` property returns a list of all job objects in the container.

           :rtype: :class:`saga.job.Job` list

        .. py:attribute:: size

           The (read-only) `size` property returns the number of job objectis in the container.

           :rtype: int

        .. py:attribute:: states

           The (read-only) `states` property returns a list of states that represent the 
           states of the individual jobs in the container. 

           :rtype: list
    """


    def __init__ (self) :

        TaskContainer.__init__(self)

        import saga.attributes as sa

        self._attributes_register   ("Jobs",   [], sa.ANY, sa.VECTOR, sa.READONLY)
        self._attributes_set_getter ("Jobs",   self.get_jobs)


    def get_jobs (self) :
        """ This is similar to get_tasks(), but returns only Job typed entries
        from the container.
        """

        tasks = self.get_tasks ()
        jobs  = []

        for task in tasks :
            if isinstance (task, Job) :
                jobs.append (task)

        return jobs


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

