create table so.msparam
(
id int identity(1,1),
paramname varchar(50)null,
paramvalue varchar(100)null,
paramtype varchar(50)null
);

insert into so.msparam(paramname,paramvalue,paramtype)values('mailprofile','devel','smtp')
insert into so.msparam(paramname,paramvalue,paramtype)values('mailrecipient','afifpratama.work@gmail.com','smtp')
insert into so.msparam(paramname,paramvalue,paramtype)values('csvdir','D:\office-project\datalake\directory\srcdir\csv-file','folder')
insert into so.msparam(paramname,paramvalue,paramtype)values('csvdirloop','D:\office-project\datalake\directory\srcdir\csv-file-loop','folder')
insert into so.msparam(paramname,paramvalue,paramtype)values('trgdir','D:\office-project\datalake\directory\trgdir','folder')