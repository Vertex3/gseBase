UPDATE [UWGISProduction].dbo.active_Floor set STACKLEVEL = (select StackLevel
  FROM [UWGISProduction].[dbo].[pubFloors] where FLOORID = [UWGISProduction].dbo.active_Floor.FloorID);