- [x] Let's NOT include "collections" information in the dashboard except for "Collections Referrals" for now. The other data (uncollectible arrearages, bad debt, and revenue from collections) is reported funny from some of the utilities. I will look into it and see if it's worth adding to the dashboard.

- [x] Rename to dashboard title to "Energy Affordability Dashboard"

- [x] Rewrite dashboard subtitle to "Residential affordability data from Washington's five investor-owned electric and gas utilities"

- [x] Create an "About" tab or section. It can include:
  - A description of what the dashboard shows, how the dashboard can be used, and the importance of having and analyzing energy affordability data. I can refine this but here is a draft for now:
  "As energy bills are increasing in Washington State and across the country, it's increasingly important for utilities to report what is happening and for the public to critically analyze the trends. The Energy Affordability Dashboard is a tool that visually and numerically tells the story of how Washington's residential energy utility customers are experiencing past-due balances, disconnections, bill assistance, and collections. With this tool you can explore geographic patterns, identify trends over time and between utilities, download data and graphics, and more."
  - Include some statement about the data analysis being done by NW Energy Coalition, like: 
  "This analysis was prepared by the NW Energy Coalition and this dashboard was prepared by Peter Mason."
  - Include a hyperlink to the source where the data comes from. Something like: 
  "Data in this dashboard comes from monthly and quarterly reports that utilities file with the Utilities & Transportation Commission in docket U-200281"
  - Include a hyperlink to another helpful resource with Washington disconnections data: 
  "For additional information on utility disconnections data and policies in Washington State and nationwide, see the Indiana University Utility Disconnections Dashboard."

- [x] In the dashboard, let's use "past-due balances" instead of "arrearages" and "utility" instead of "IOU" because most people don't know what an "arrearage" or "IOU" is. (However, I'm using the terms interchangeable in these instructions for ease.)

- [x] Make the highlighted data in the 4 rectangles at the top key statewide (i.e., across all 5 IOUs) metrics, including:
  - Total number of customers with arrearages up to that point in the year
  - Total $ value of arrearages reported up to that point in the year
  - Total number of disconnections reported up to that point in the year
  - Total bill assistance funds (both LIHEAP and utility assistance) distributed up to that point in the year

- [x] Add a key that can be seen no matter what tab you're on that spells out the utilities' acronyms or write the full name:
  - Avista
  - Puget Sound Energy (PSE)
  - Pacific Power (PAC)
  - Cascade Natural Gas (CNG)
  - Northwest Natural Gas (NWN)

- [ ] Include graph titles, legends, and y-axis labels in all visuals.

- [ ] Reorder the tabs, from left to right, and combine the currently existing tabs by topic as described below. I've included instructions on the content for each tab below as well as (imperfect) example visuals for reference.

## Tab 1: Past-Due Balances

- [ ] Keep this stacked line graph from the existing "Arrearage Counts" tab. 
  - Title: Number of Customers with Past-Due Balances by Utility
  - Y-axis: Number of Customers

- [ ] Add a line graph with a trendline (no need to have the slope formula) for arrearage counts for each utility. This will be 5 separate line graphs. It could potentially be made into a grid some pattern where they're side by side so as to make the comparison easier.
  - Title: Number of [insert the specific utility name] Customers with Past-Due Balances
  - Y-axis: Number of Customers

- [ ] Add a stacked line graph showing the total amount of past-due balances for all utilities.
  - Title: Total Amount of Past-Due Balances
  - Y-axis: Past-Due Balances ($USD)

- [ ] Add a line chart that has a line for each of the 5 IOUs showing the past-due balances in March of each year
  - Title: Past-Due Balances by Utility in March of Each Year
  - Y-axis: Past-Due Balances ($USD)

- [ ] Add a line chart that has a line for each of the 5 IOUs showing the average past-due balance in March of each year
  - Title: Average Past-Due Balance by Utility in March of Each Year
  - Y-axis: Average Past-Due Balance ($USD)

- [ ] Add a stacked bar chart with a trendline that shows the total arrearages for all 5 IOUs by vintage. I like the colors here because lighter blue means not as old and darker means more old.
  - Title: Past-Due Balances by Days Past Due
  - Y-axis: Past-Due Balance ($USD)

- [ ] Add a pie chart for the most recent full year of data that shows the total arrearages for all 5 IOUs by vintages. The example visual below shows this for only a single month. 
  - Title: Percentage of Past-Due Balances by Days Past Due
  - Label: Include the total value of arrearages and percentage of total for each arrearage as shown below

### KLI Arrearage Amounts

- [ ] Add a stacked bar chart with a trendline that shows the total KLI arrearages for all 5 IOUs by vintage. I like the colors here because lighter blue means not as old and darker means more old.
  - Title: Low-Income Past-Due Balances by Days Past Due
  - Y-axis: Past-Due Balance ($USD)

- [ ] Add a pie chart for the most recent full year of data that shows the total low-income arrearages for all 5 IOUs by vintages. The example visual below shows this for only a single month. 
  - Title: Percentage of Low-Income Past-Due Balances by Days Past Due
  - Label: Include the total value of arrearages and percentage of total for each arrearage as shown below

- [ ] Add a clustered bar chart that shows low-income arrearages by vintage in March of each year
  - Title: Low-Income Past-Due Balances by Days Past Due in March
  - Y-axis: Past-Due Balance ($USD)

## Tab 2: Disconnections

- [ ] Add a stacked line graph showing the number of disconnections by IOU
  - Title: Number of Disconnections by Utility
  - Y-axis: Number of Disconnected Customers

- [ ] Add a single line graph with a trendline that shows the total number if disconnections in each year (starting in 2022) across all 5 IOUs
  - Title: Total Number of Disconnections
  - Y-axis: Number of Disconnected Customers

- [ ] Add a stacked line graph showing the number of customers receiving disconnection notices by IOU
  - Title: Number of Customers Receiving Disconnection Notices by Utility
  - Y-axis: Number of Customers

## Tab 3: Bill Assistance

- [ ] Add a stacked line graph showing the number of customers enrolled in a bill assistance program by IOU
  - Title: Number of Customers Enrolled in a Bill Assistance Program by Utility
  - Y-axis: Number of Customers

- [ ] Add a line graph with a trendline (no need to have the slope formula) for the number of customers enrolled in a bill assistance program for each utility. This will be 5 separate line graphs. It could potentially be made into a grid some pattern where they're side by side so as to make the comparison easier.
  - Title: Number of [insert the specific utility name] Customers Enrolled in a Bill Assistance Program
  - Y-axis: Number of Customers

- [ ] Add a stacked bar chart showing the amount of distributed bill assistance dollars from LIHEAP by IOU
  - Title: Distributed Bill Assistance Dollars from LIHEAP
  - Y-axis: $USD

- [ ] Add a stacked bar chart showing the amount of distributed bill assistance dollars from utility assistance programs by IOU
  - Title: Distributed Bill Assistance Dollars from Utility Programs
  - Y-axis: $USD

- [ ] Add a stacked line graph showing the number of customers with payment arrangements by IOU
  - Title: Number of Customer with Payment Arrangements by Utility
  - Y-axis: Number of Customers

- [ ] Add a line graph with a trendline (no need to have the slope formula) for the number of customers with payment arrangements for each utility. This will be 5 separate line graphs. It could potentially be made into a grid some pattern where they're side by side so as to make the comparison easier.
  - Title: Number of [insert the specific utility name] Customers with Payment Arrangements
  - Y-axis: Number of Customers

## Tab 4: Collections 

- [ ] Add a stacked line graph showing the number of customers referred to collection agencies by IOU
  - Title: Number of Customers Referred to Collection Agencies by Utility
  - Y-axis: Number of Customers

- [ ] Delete "Uncollectible Arrears"

- [ ] Let's include bill assistance funds from LIHEAP and from utility programs. I told you we'd put this on the backburner, but I change my mind. This info is more important and what advocates will be very interested to see. I will work to add this into the master spreadsheet.


## Styling

- [x] For graphics, use the following colors for each utility (based on their company colors/logo):
  - Avista: navy blue
  - PSE: teal
  - PAC: red
  - Cascade: grey
  - NWN: light green

- [ ] The "Data Table" can be organized similarly to how it is. Starting with utilities, then zip code, then month, then the data value (e.g., number if disconnections). However, can the months be organized in chronological order for each zip code?
