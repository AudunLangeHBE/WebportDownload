
/* Først må database opprettes! - så kjøres skriptet nedenfor */


-- Opprett tabeller:

CREATE TABLE dbo.Undersøkelse(
	Id int NULL,
	Guid uniqueidentifier NULL,
	Title varchar(100) NULL,
	CompanyId varchar(50) NULL
)


CREATE TABLE dbo.spørsmål(
	id int NULL,
	[text] varchar(200) NULL,
	[type] varchar(50) NULL,
	ParentFormID int NULL,
	sort int NULL,
	selectionlistid int NULL
)

CREATE TABLE dbo.svaralternativ(
	ParentFormId int NOT NULL,
	Id int NOT NULL,
	FieldValue varchar(100) NULL,
	FieldText varchar(100) NULL,
	Sort int NULL,
	SelectionListId int NULL
) 

CREATE TABLE dbo.svar(
	ParentFormId int NULL,
	FormGuid uniqueidentifier NULL,
	CompanyId varchar(50) NULL,
	CultureShort nchar(10) NULL,
	CreatedDate datetime NULL,
	FormFieldId int NULL,
	OriginalText varchar(200) NULL,
	LanguageText varchar(100) NULL,
	SelectedValueText varchar(100) NULL,
	[Value] nvarchar(max) NULL,
	TidspunktNedlastet datetime NULL
)

CREATE TABLE dbo.webportCreateSet(
	FormId int NULL,
	CompanyId varchar(50) NULL,
	ExportSetId int NULL,
	ExportGuid uniqueidentifier NULL,
	FormCount int NULL,
	FormFieldCount int NULL,
	tidspunkt datetime NULL,
	DownloadComplete int NULL
)

CREATE TABLE dbo.Bakgrunnsdata(
	FormGuid uniqueidentifier NULL,
	AgeGroup nvarchar(50) NULL,
	Gender nvarchar(50) NULL,
	WardReshId bigint NULL,
	DepartmentReshId bigint NULL,
	HospitalId bigint NULL,
	LocationReshId bigint NULL
)

CREATE TABLE dbo.LogDownload(
	tidspunkt datetime NULL,
	exitCode int NULL,
	textValue nvarchar(250) NULL
)


CREATE TABLE dbo.Undersøkelse_import(
	Id int NULL,
	Guid uniqueidentifier NULL,
	Title varchar(100) NULL,
	CompanyId varchar(50) NULL
)

CREATE TABLE dbo.spørsmål_import(
	Id int NOT NULL,
	[Text] varchar(200) NULL,
	[Type] varchar(50) NULL,
	ParentFormId int NULL,
	Sort int NULL,
	SelectionListId int NULL
) 

CREATE TABLE dbo.svaralternativ_import(
	ParentFormId int NOT NULL,
	Id int NOT NULL,
	FieldValue varchar(100) NULL,
	FieldText varchar(100) NULL,
	Sort int NULL,
	SelectionListId int NULL
)

CREATE TABLE dbo.svar_import(
	ParentFormId int NULL,
	FormGuid uniqueidentifier NULL,
	CompanyId varchar(50) NULL,
	CultureShort nchar(10) NULL,
	CreatedDate datetime NULL,
	FormFieldId int NULL,
	OriginalText varchar(200) NULL,
	LanguageText varchar(100) NULL,
	SelectedValueText varchar(100) NULL,
	[Value] nvarchar(max) NULL
)

CREATE TABLE dbo.webportCreateSet_import(
	FormId int NULL,
	CompanyId varchar(50) NULL,
	ExportSetId int NULL,
	ExportGuid uniqueidentifier NULL,
	FormCount int NULL,
	FormFieldCount int NULL
) 

CREATE TABLE dbo.Bakgrunnsdata_import(
	FormGuid uniqueidentifier NULL,
	Age bigint NULL,
	AgeGroup nvarchar(50) NULL,
	Gender nvarchar(50) NULL,
	WardId nvarchar(50) NULL,
	WardReshId nvarchar(50) NULL,
	DepartmentId nvarchar(50) NULL,
	DepartmentReshId nvarchar(50) NULL,
	LocationId nvarchar(50) NULL,
	LocationReshId nvarchar(50) NULL,
	HospitalId nvarchar(50) NULL,
	MorsDate datetime NULL
)

GO

-- Opprett prosedyrer:

create Procedure dbo.InsertLog  @code int, @msg nvarchar(250)
as
begin
	Insert into dbo.LogDownload(tidspunkt, exitCode, textValue)
	Select GetDate(), @code, @msg
	 ;
end;
Go

CREATE PROCEDURE dbo.Truncate_Import_Tables
AS
BEGIN
	SET NOCOUNT ON;
	-- Truncate table dbo.UndersøkelsesListe_import;
	Truncate table dbo.Undersøkelse_import;
	Truncate table dbo.spørsmål_import;
	Truncate table dbo.svaralternativ_import;
	Truncate table dbo.webportCreateSet_Import;
	Truncate table dbo.svar_import;
	Truncate table dbo.Bakgrunnsdata_import;
END
GO

CREATE PROCEDURE dbo.SetCreatesetCompleted @setId int
AS
BEGIN
	Update dbo.webportCreateSet set DownloadComplete=1 where ExportSetId=@setId;
	return 0;
END
GO



CREATE PROCEDURE dbo.Oppdater_undersøkelser_fra_import
AS
BEGIN
	SET NOCOUNT ON;

	With diff as 
	(
		Select Id , Guid from dbo.Undersøkelse_import
		  except
		Select Id , Guid from dbo.Undersøkelse
	)
	insert into dbo.Undersøkelse (id, GUID, Title, CompanyId)
	select imp.Id, imp.GUID, imp.Title, imp.CompanyId
	from diff join dbo.Undersøkelse_import  imp on diff.id=imp.Id and diff.guid=imp.guid
	;

END
GO

CREATE PROCEDURE dbo.Oppdater_spørsmål_fra_import
AS
BEGIN
	SET NOCOUNT ON;

	With diff as 
	(
		Select Id, ParentFormID from dbo.spørsmål_import
		  except
		Select Id, ParentFormID from dbo.spørsmål
	)
	insert into dbo.spørsmål (id, text, type, ParentFormID, sort, selectionlistid)
	select imp.Id, imp.text, imp.type, imp.ParentFormID, imp.sort, imp.selectionlistid
	from diff join dbo.spørsmål_import  imp on diff.id=imp.Id and diff.ParentFormID=imp.ParentFormID
	;

END
GO

Create PROCEDURE dbo.Oppdater_svaralternativ_fra_import
AS
BEGIN
	SET NOCOUNT ON;

	With diff as 
	(
		Select ParentFormID, Id, SelectionListId from dbo.svaralternativ_import
		  except
		Select  ParentFormID, Id, SelectionListId from dbo.svaralternativ
	)
	insert into dbo.svaralternativ 
	(ParentFormId, Id, FieldValue, FieldText, Sort, SelectionListId)
	select imp.ParentFormId, imp.Id, imp.FieldValue, imp.FieldText, imp.Sort, imp.SelectionListId
	from diff join dbo.svaralternativ_import  imp on diff.ParentFormID=imp.ParentFormID and diff.Id=imp.Id and diff.SelectionListId=imp.SelectionListId
	;

END
GO

CREATE PROCEDURE dbo.Oppdater_svar_fra_import
AS
BEGIN
	SET NOCOUNT ON;

	Declare @tidspunkt datetime; set @tidspunkt=GetDate();
	With diff as 
	(
		Select ParentFormID, FormGUID, FormFieldId from dbo.svar_import
		  except
		Select  ParentFormID, FormGUID, FormFieldId from dbo.svar
	)
	insert into dbo.svar (ParentFormId, FormGuid, CompanyId, CultureShort, CreatedDate, FormFieldId, OriginalText, SelectedValueText, Value, TidspunktNedlastet)
	select imp.ParentFormId, imp.FormGuid, imp.CompanyId, imp.CultureShort, imp.CreatedDate, imp.FormFieldId, imp.OriginalText, imp.SelectedValueText, imp.Value, @tidspunkt
	from diff join dbo.svar_import  imp on diff.ParentFormID=imp.ParentFormID and diff.FormGuid=imp.FormGuid and diff.FormFieldId=imp.FormFieldId
	;

END
GO

CREATE Procedure dbo.Oppdater_Createset_fra_import
as
Begin
	With diff as 
	(
		Select FormId, CompanyId, ExportSetId, ExportGuid from dbo.webportCreateSet_import
		  except
		Select FormId, CompanyId, ExportSetId, ExportGuid from dbo.webportCreateSet
	)
	Insert into dbo.webportCreateSet(FormId, CompanyId, ExportSetId, ExportGuid, FormCount, FormFieldCount, tidspunkt)
	 select Imp.FormId, Imp.CompanyId, Imp.ExportSetId, Imp.ExportGuid, Imp.FormCount, Imp.FormFieldCount, GetDate()
	from diff join dbo.webportCreateSet_import imp on 
	diff.FormId=imp.FormId and diff.CompanyId=imp.CompanyId 
	and diff.ExportSetId=imp.ExportSetId and diff.ExportGuid=imp.ExportGuid;
	;

End
Go

CREATE Procedure dbo.Oppdater_bakgrunnsdata_fra_import
as
begin
	SET NOCOUNT ON;

	if Object_id('tempdb..#nye') is not null drop table #nye;

	With diff as 
	( 
		select Cast(FormGuid as UniqueIdentifier) FormGuid from dbo.Bakgrunnsdata_import
		EXCEPT
		select formGUID from dbo.Bakgrunnsdata
	)
	select FormGuid
	into #nye
	from diff;

	Insert into dbo.Bakgrunnsdata(FormGuid, AgeGroup, Gender, WardReshId, DepartmentReshId, LocationReshId, HospitalId)
	Select src.FormGuid, src.AgeGroup, src.Gender, src.WardReshId, src.DepartmentReshId, src.LocationReshId, src.HospitalId
	from #nye n join dbo.Bakgrunnsdata_import src on n.FormGuid=src.FormGuid;

end

GO

