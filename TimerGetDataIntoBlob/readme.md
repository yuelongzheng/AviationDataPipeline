# Get Data Into Blob - Python

This Timer trigger gets data into the container dataset1

## How it works

For a `TimerTrigger` to work, you provide a schedule in the form of a [cron expression](https://en.wikipedia.org/wiki/Cron#CRON_expression)(See the link for full details). A cron expression is a string with 6 separate expressions which represent a given schedule via patterns. The pattern we use to represent every 5 minutes is `0 */5 * * * *`. This, in plain text, means: "When seconds is equal to 0, minutes is divisible by 5, for any hour, day of the month, month, day of the week, or year".

The Timer Trigger runs the program based on the UTC timezone. so check the [UTC] (https://www.timeanddate.com/worldclock/timezone/utc)() timezone to check when the trigger goes off.

There must be atleast a 10 hour delay between the running of the timer trigger and when the trigger is set off, in the case the day of the month of the trigger is set.
For example if the cron expression is '0 0 0 15 * *', then the trigger must be deployed and ran 10 hours before midnight (UTC time).



