
Part 4: Adding File Transfer
****************************

In this fourth part of the tutorial, we again build on the previous example and
some code that copies our job's output file back to the local machine. This is
done using the saga.filesystem API package.

Prerequisites
-------------

This example assumes that you have SFTP access to the remote resource that you
have used in the previous example. Again, this example assumes that you have a
working public/private SSH key-pair and that you can sftp into your remote
resource using those keys, i.e., your public key is in the
~/.ssh/authorized_hosts file on the remote machine. If
you are not sure how this works, you might want to read about 
`SSH and GSISSH <https://github.com/radical-cybertools/radical.saga/wiki/SSH-and-GSISSH>`_ 
first.

Hands-On: Remote Job Submission with File Staging
=================================================

Copy the code from the previous example 3 to a new file ``saga_example_remote_staging.py``.
Add the following code after the last print, right before the except statement:

.. note:: Make sure that you adjust the paths to reflect your home directory on the remote machine.

.. code-block:: python

    outfilesource = 'sftp://gw68.quarry.iu.teragrid.org/users/oweidner/mysagajob.stdout'
    outfiletarget = 'file://localhost/tmp/'
    out = saga.filesystem.File(outfilesource, session=ses)
    out.copy(outfiletarget)

    print "Staged out %s to %s (size: %s bytes)" % (outfilesource, outfiletarget, out.get_size())


Run the Code
------------

Save the file and execute it (**make sure your virtualenv is activated**):

.. code-block:: bash

    python saga_example_remote.py

The output should look something like this:

.. code-block:: none

    Job ID    : None
    Job State : New

    ...starting job...

    Job ID    : [ssh://gw68.quarry.iu.teragrid.org]-[18533]
    Job State : Done

    ...waiting for job...

    Job State : Done
    Exitcode  : 0

    Staged out gw68.quarry.iu.teragrid.org/users/oweidner/mysagajob.stdout to file://localhost/tmp/ (size: 16 bytes)


Check the Output
----------------

Your output file should now be in /tmp/mysagajob.stdout and contain the 
string ``Hello from SAGA``.

Coding URLs
-----------

URLs ``sftp://username:password@hostname:port//some/path`` and
``sftp://username:password@hostname:port/some/path`` (note the two ``//`` in
the first URL) are supposed to be the same. However, SAGA may treat them
diffently due some missing path normalization. URL should be coded with single
'/' or composed as follow:

.. code-block:: python

    remote_dir_url = saga.Url()
    remote_dir_url.scheme = 'sftp'
    remote_dir_url.username = username
    remote_dir_url.username = $PASSWORD
    remote_dir_url.host = hostname
    remote_dir_url.port = port
    remote_dir_url.path = /some/path

