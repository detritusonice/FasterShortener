# FasterShortener
Performance Improving touches to a live URL Shortener platform

These are some of the changes I made on a popular shortener WebApp to improve space requirements by a factor of 10000 and statistics reporting delay by a similar amount.

Gathering raw data per click works when traffic is low. But on sites of a million clicks per day or more, the database grows too large, too fast, and the app slows down, the backup process slows down, and at some point breaks down.
In the particular application, the administrator panel shows a fair bit of full-site statistics. 

The result is that either you wait them out (takes minutes to display the page) or you tear them down. OR you just find a better way to do things. Noone exports raw data of multimillions of rows. No site this size can afford storing billions of raw click data records.

So this projects condenses raw data down to what is useful for the application to work normally, by creating some tables to store the appropriate statistics per url, click, day, country, operating system, browser and referrer site. Then uses a script to update those tables from the raw data every x minutes, then wipe the raw data out.

This is the only portion of the work that can be published. All others are modifications on proprietary code.
