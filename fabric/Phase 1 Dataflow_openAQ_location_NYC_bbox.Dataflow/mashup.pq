[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared openaq_NYC_raw = let
    BaseUrl = "https://api.openaq.org/",
    ApiKey = "9ed5431dd41b3a15c6c124e9774de74a680a1e1f2ec50631c2b95f37cbedc50a",
    Limit = 1000,

    FnGetPage = (page as number) =>
        Json.Document(
            Web.Contents(
                BaseUrl,
                [
                    RelativePath = "v3/locations",
                    Query = [
                        bbox = "-74.454622,40.496240,-73.527020,41.038594",
                        page = Text.From(page),
                        limit = Text.From(Limit)
                    ],
                    Headers = [
                        #"X-API-Key" = ApiKey,
                        Accept = "application/json"
                    ]
                ]
            )
        ),

    FirstPage = FnGetPage(1),
    TotalFound = FirstPage[meta][found],
    TotalPages = Number.RoundUp(TotalFound / Limit),
    Pages = {1..TotalPages},

    RawPages = List.Transform(Pages, each FnGetPage(_)[results]),
    Combined = List.Combine(RawPages),

    Source = Table.FromList(Combined, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1" = Table.ExpandRecordColumn(Source, "Column1", {"id", "name", "locality", "timezone", "country", "owner", "provider", "isMobile", "isMonitor", "instruments", "sensors", "coordinates", "licenses", "bounds", "distance", "datetimeFirst", "datetimeLast"}, {"id", "name", "locality", "timezone", "country", "owner", "provider", "isMobile", "isMonitor", "instruments", "sensors", "coordinates", "licenses", "bounds", "distance", "datetimeFirst", "datetimeLast"}),
  #"Expanded country" = Table.ExpandRecordColumn(#"Expanded Column1", "country", {"id", "code", "name"}, {"id.1", "code", "name.1"}),
  #"Expanded sensors" = Table.ExpandListColumn(#"Expanded country", "sensors"),
  #"Expanded sensors 1" = Table.ExpandRecordColumn(#"Expanded sensors", "sensors", {"id", "name", "parameter"}, {"sensor_id", "sensor_name", "sensor_parameter"}),
  #"Expanded sensor_parameter" = Table.ExpandRecordColumn(#"Expanded sensors 1", "sensor_parameter", {"id", "name", "units", "displayName"}, {"sensor_parameter_id", "sensor_parameter_name", "sensor_parameter_units", "sensor_parameter_displayName"}),
  #"Expanded coordinates" = Table.ExpandRecordColumn(#"Expanded sensor_parameter", "coordinates", {"latitude", "longitude"}, {"latitude", "longitude"}),
  #"Expanded datetimeFirst" = Table.ExpandRecordColumn(#"Expanded coordinates", "datetimeFirst", {"utc", "local"}, {"datetimeFirst_utc", "datetimeFirst_local"}),
  #"Expanded datetimeLast" = Table.ExpandRecordColumn(#"Expanded datetimeFirst", "datetimeLast", {"utc", "local"}, {"datetimeLast_utc", "datetimeLast_local"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded datetimeLast", {{"name", type text}, {"locality", type text}, {"timezone", type text}, {"code", type text}, {"name.1", type text}, {"isMobile", type logical}, {"isMonitor", type logical}, {"sensor_name", type text}, {"sensor_parameter_name", type text}, {"sensor_parameter_units", type text}, {"sensor_parameter_displayName", type text}, {"latitude", type number}, {"longitude", type number}, {"datetimeFirst_utc", type datetime}, {"datetimeFirst_local", type datetimezone}, {"datetimeLast_utc", type datetime}, {"datetimeLast_local", type datetimezone}, {"sensor_parameter_id", Int64.Type}, {"id", Int64.Type}, {"id.1", Int64.Type}, {"sensor_id", Int64.Type}})

in
    #"Changed column type";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "75f1c875-0ee7-47ad-a0a7-385b33d47af7"]}[Data]{[lakehouseId = "64e04aa9-1d1a-4e60-864a-cf577f69cd39"]}[Data];
