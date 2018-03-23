# Mirai
Mirai


### Known Issue
When Python daemon `scheduler.run` start as an Linux service via Systemd, the I\O redirect for STDIN STDOUT and STDERR does not work, the output does not flushed to files which duplicate STD IO file discriptor.

#### Solution
Start the daemon manually, the STD IO worked as expected.

Still not found the root cause.


## TODO
Find a common way to decorator normal method and instance method, decorator should be **thread-safe**
