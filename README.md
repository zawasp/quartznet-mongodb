# Quartz.MongoDB

MongoDB job store implementation for [Quartz.Net](https://github.com/quartznet/quartznet). 

----------

###Install
You can install it via nuget

    Install-Package Quartz.MongoDB

###Usage
If you are configuring Quartz in app.config or web.config, you can configure as follows: 

    <quartz>  
	    <add key="quartz.jobStore.type" value="Quartz.MongoDB.JobStore, Quartz.MongoDB"/>
	    <add key="quartz.jobStore.connectionString" value="mongodb://localhost/quartz-db"/>
	</quartz>
or

    <connectionStrings>
	    <add name="ConnectionString1" connectionString="mongodb://localhost/quartz-db"/>
	</connectionStrings>
	<quartz>  
	    <add key="quartz.jobStore.type" value="Quartz.MongoDB.JobStore, Quartz.MongoDB"/>
	    <add key="quartz.jobStore.connectionStringName" value="ConnectionString1"/>
	</quartz>
If you are configuring in code, you may use as follows:

    var properties = new NameValueCollection();
    properties["quartz.jobStore.type"] = "Quartz.MongoDB.JobStore, Quartz.MongoDB";
    properties["quartz.jobStore.connectionString"] = "mongodb://localhost/quartz-db";
    var scheduler = new StdSchedulerFactory(properties).GetScheduler();
