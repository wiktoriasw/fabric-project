[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "openaq_days_yearly_raw_1_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "value", DestinationColumnName = "value"], [SourceColumnName = "id", DestinationColumnName = "id"], [SourceColumnName = "name", DestinationColumnName = "name"], [SourceColumnName = "units", DestinationColumnName = "units"], [SourceColumnName = "utc", DestinationColumnName = "utc"], [SourceColumnName = "utc.1", DestinationColumnName = "utc.1"], [SourceColumnName = "q02", DestinationColumnName = "q02"], [SourceColumnName = "q25", DestinationColumnName = "q25"], [SourceColumnName = "q75", DestinationColumnName = "q75"], [SourceColumnName = "q98", DestinationColumnName = "q98"], [SourceColumnName = "sensor_id", DestinationColumnName = "sensor_id"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared openaq_days_yearly_raw_1 = let
    BaseUrl = "https://api.openaq.org/",
    ApiKey  = "9ed5431dd41b3a15c6c124e9774de74a680a1e1f2ec50631c2b95f37cbedc50a",
    Limit   = 1000,

    // ✅ DLA DAILY: UŻYWAJ MAŁYCH OKIEN CZASU (np. 30 dni)
    // Przykład: jeden miesiąc. Potem robisz kolejne miesiące/batche.
    DateFrom = "2025-01-01T00:00:00Z",
    DateTo   = "2025-12-31T23:59:59Z",

    // Ochrona przed 429
    BatchSize = 15,         // daily: zacznij od 5–10
    BatchIndex = 14,       //242/10 = 25 razy cały rok, od 0

    DelaySeconds = 5,       // daily: 5s jest bezpieczniejsze niż 3s
    MaxPagesPerSensor = 50, // daily może mieć więcej stron
    MaxRetries429 = 4,      // retry na 429

    SensorIds = {
        671,673,674,1097,1098,1099,1102,1103,1106,1121,1128,1143,1145,1146,1147,
        1152,1522,1523,1534,1535,1536,1693,1758,1761,1777,2016,2018,2644,2645,
        2646,3637,3638,3639,3640,3950,3951,3952,3953,3954,23341,25520,1662910,
        2165515,2189530,2780575,4272080,4272086,4272224,4272250,4272273,4272290,
        4272352,4272431,5077566,5077821,6530188,6530200,6530237,6530245,6530280,
        6530283,7254050,7254073,7324143,7324145,7386938,7386942,7762999,7763001,
        7815388,7815389,7897332,7897339,7897354,7897355,7970808,7970916,7970930,
        7970982,7971030,7971073,7971082,7971117,7971145,7971247,7971262,7971292,
        7971316,7971373,7971629,7971668,7971870,7971901,7972258,7972295,7972608,
        7972609,7972670,7972671,7972696,7972702,7972703,7972707,7978166,7978193,
        7978991,7979020,7979380,7979399,7979411,7979412,7979444,7979475,7979816,
        7979835,7979861,7979870,7979901,7979909,8520920,8520925,8520933,8520935,
        8520936,8520938,9244019,9244020,9244025,9244029,9244031,9244033,9648774,
        9648782,9648783,9648796,9648811,9648812,10454036,10454043,10454057,
        10454068,10454078,10454082,10888539,10888556,10888566,10888578,10888579,
        10888589,10986460,10986461,10986462,10986463,10986465,10986466,10986467,
        11024296,11024299,11024300,11024303,11024304,11024305,11074566,11074571,
        11074572,11074582,11074585,11074587,11571192,11571193,11571194,11571197,
        11571199,11571202,12178175,12178176,12178177,12178178,12178491,12212726,
        12212727,12212728,12212729,12212730,12514431,12514432,12514433,13071824,
        13071825,13071826,13071827,13071828,13193868,13193869,13193870,13193871,
        13193872,13193873,13193874,13193875,13193876,13193877,13322774,13322775,
        13322776,13322777,13322778,13594426,13594427,13594428,13594429,13594430,
        13719588,13719589,13719590,13719591,13719592,14152914,14152915,14152916,
        14152917,14152918,14280311,14280312,14280313,14280314,14280315,14349090,
        14349091,14349092,14349093,14349094,14536763,14536764,14536765,14536766,
        14536767,14904962,14904963,14904964,14904965,14904966
    },

   

    // buffer listy sensorów
    SensorIdsBuffered = List.Buffer(SensorIds),

    // klucz: wybór paczki sensorów
    SensorBatch = List.Range(SensorIdsBuffered, BatchIndex * BatchSize, BatchSize),

    MakeQuery = (page as number) as record =>
        [
            page = Text.From(page),
            limit = Text.From(Limit),
            date_from = DateFrom,
            date_to = DateTo
        ],

    TryFetch = (sensorId as number, page as number) as record =>
        let
            Response =
                try
                    Web.Contents(
                        BaseUrl,
                        [
                            RelativePath = "v3/sensors/" & Text.From(sensorId) & "/days",
                            Query = MakeQuery(page),
                            Headers = [
                                #"X-API-Key" = ApiKey,
                                Accept = "application/json"
                            ],
                            ManualStatusHandling = {429}
                        ]
                    )
                otherwise
                    null,

            Meta = if Response <> null then Value.Metadata(Response) else null,
            Status = if Meta <> null and Record.HasFields(Meta, "Response.Status") then Meta[Response.Status] else null,
            RespHeaders = if Meta <> null and Record.HasFields(Meta, "Headers") then Meta[Headers] else null,
            RetryAfter =
                if RespHeaders <> null and Record.HasFields(RespHeaders, "Retry-After") then
                    try Number.FromText(RespHeaders[#"Retry-After"]) otherwise null
                else
                    null,

            Body =
                if Response <> null and Status <> 429 then
                    try Json.Document(Response) otherwise null
                else
                    null
        in
            [status = Status, retryAfter = RetryAfter, body = Body],

    FnGetPage = (sensorId as number, page as number) as list =>
        let
            Go = (attempt as number) as record =>
                let
                    _ = Function.InvokeAfter(() => 0, #duration(0,0,0,DelaySeconds)),
                    R = TryFetch(sensorId, page),
                    WaitSeconds =
                        if R[status] = 429 then
                            (if R[retryAfter] <> null then R[retryAfter] else (10 * (attempt + 1)))
                        else
                            0,
                    Next =
                        if R[status] = 429 and attempt < MaxRetries429 then
                            Function.InvokeAfter(() => @Go(attempt + 1), #duration(0,0,0,WaitSeconds))
                        else
                            R
                in
                    Next,

            Final = Go(0),
            Results =
                if Final[body] <> null and Record.HasFields(Final[body], "results") then
                    Final[body][results]
                else
                    {}
        in
            Results,

    FnGetAllPagesForSensor = (sensorId as number) as table =>
        let
            Pages =
                List.Generate(
                    () => [p = 1, res = FnGetPage(sensorId, 1)],
                    each [p] <= MaxPagesPerSensor and List.Count([res]) > 0,
                    each [p = [p] + 1, res = FnGetPage(sensorId, [p] + 1)],
                    each [res]
                ),
            Combined = List.Combine(Pages),
            AsTable =
                if List.Count(Combined) = 0 then
                    #table({"row", "sensor_id"}, {})
                else
                    Table.AddColumn(
                        Table.FromList(Combined, Splitter.SplitByNothing(), {"row"}, null, ExtraValues.Error),
                        "sensor_id",
                        each sensorId,
                        Int64.Type
                    )
        in
            AsTable,

    AllSensorsTables = List.Transform(SensorBatch, each FnGetAllPagesForSensor(_)),
    Source = Table.Combine(AllSensorsTables),

  #"Expanded row" = Table.ExpandRecordColumn(Source, "row", {"value", "parameter", "period", "coordinates", "summary"}, {"value", "parameter", "period", "coordinates", "summary"}),
  #"Expanded period" = Table.ExpandRecordColumn(#"Expanded row", "period", {"datetimeFrom", "datetimeTo"}, {"datetimeFrom", "datetimeTo"}),
  #"Expanded datetimeFrom" = Table.ExpandRecordColumn(#"Expanded period", "datetimeFrom", {"utc", "local"}, {"utc", "local"}),
  #"Expanded datetimeTo" = Table.ExpandRecordColumn(#"Expanded datetimeFrom", "datetimeTo", {"utc", "local"}, {"utc.1", "local.1"}),
  #"Expanded summary" = Table.ExpandRecordColumn(#"Expanded datetimeTo", "summary", {"q02", "q25", "q75", "q98"}, {"q02", "q25", "q75", "q98"}),
  #"Expanded parameter" = Table.ExpandRecordColumn(#"Expanded summary", "parameter", {"id", "name", "units"}, {"id", "name", "units"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded parameter", {{"value", type number}, {"id", Int64.Type}, {"name", type text}, {"units", type text}, {"utc", type datetime}, {"local", type datetimezone}, {"utc.1", type datetime}, {"local.1", type datetimezone}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type", {"coordinates"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed columns", {{"q02", type number}, {"q25", type number}, {"q75", type number}, {"q98", type number}, {"sensor_id", Int64.Type}})
in
    #"Changed column type 1";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "75f1c875-0ee7-47ad-a0a7-385b33d47af7"]}[Data]{[lakehouseId = "64e04aa9-1d1a-4e60-864a-cf577f69cd39"]}[Data];
shared openaq_days_yearly_raw_1_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "75f1c875-0ee7-47ad-a0a7-385b33d47af7"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "64e04aa9-1d1a-4e60-864a-cf577f69cd39"]}[Data],
  TableNavigation = Navigation_2{[Id = "openaq_days_yearly_raw_11", ItemKind = "Table"]}[Data]
in
  TableNavigation;
