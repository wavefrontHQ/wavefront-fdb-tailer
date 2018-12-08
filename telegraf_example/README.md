## Sample FDB Health Dashboard for Wavefront

We have included the json data for our FDB Health dashboard in this repo as ```sampleFDBHealthDashboard.json```.  This is the dashboard we use internally to monitor FDB, as shown in the talk at the FDB Summit.  It combines metrics from this application with those from ```parse-fdb-status.py```.  There might therefore be some missing metrics and empty charts when first imported in Wavefront for this and other reasons (e.g. a custom prefix is being used, FDB is running on non-standard ports, etc.).  However, we have attempted to make it as generic as possible for easy modifcation and adoption.

In order to import a dashboard into wavefront, paste the contents of the json file into the [REST API Create Dashboard](https://mon.wavefront.com/api-docs/ui/#!/Dashboard/createDashboard) body.  After clicking ```Try it out!```, the dashboard will appear in your list of dashboards.
