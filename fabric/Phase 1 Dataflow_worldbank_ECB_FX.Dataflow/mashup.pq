[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared WorldBank = let
  Source = Json.Document(Web.Contents("https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json")),
  #"Converted to table" = Table.FromList(Source, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  Navigation = #"Converted to table"{1}[Column1],
  #"Converted to table 1" = Table.FromList(Navigation, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to table 1", "Column1", {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}, {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}),
  #"Expanded country" = Table.ExpandRecordColumn(#"Expanded Column1", "country", {"id", "value"}, {"id", "value.1"}),
  #"Expanded indicator" = Table.ExpandRecordColumn(#"Expanded country", "indicator", {"id", "value"}, {"id.1", "value.2"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded indicator", {{"id.1", type text}, {"value.2", type text}, {"id", type text}, {"value.1", type text}, {"countryiso3code", type text}, {"date", type date}, {"value", type number}})
in
  #"Changed column type";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "75f1c875-0ee7-47ad-a0a7-385b33d47af7"]}[Data]{[lakehouseId = "64e04aa9-1d1a-4e60-864a-cf577f69cd39"]}[Data];
