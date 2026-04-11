# Data Dictionary

## Raw AIS Fields

| Field | Type | Description |
|-------|------|-------------|
| MMSI | Long | Maritime Mobile Service Identity (9-digit vessel ID) |
| BaseDateTime | String | Timestamp of position report |
| LAT | Double | Latitude in decimal degrees |
| LON | Double | Longitude in decimal degrees |
| SOG | Double | Speed Over Ground in knots |
| COG | Double | Course Over Ground in degrees (0-360) |
| Heading | Double | True heading in degrees |
| VesselName | String | Vessel name |
| IMO | String | International Maritime Organization number |
| CallSign | String | Radio call sign |
| VesselType | Integer | AIS vessel type code |
| Status | Integer | Navigation status code |
| Length | Double | Vessel length in meters |
| Width | Double | Vessel width in meters |
| Draft | Double | Vessel draft in meters |
| Cargo | Integer | Cargo type code |
| TransceiverClass | String | AIS transceiver class (A or B) |

## Normalized Column Names (Bronze/Silver)

| Raw Name | Normalized Name |
|----------|----------------|
| MMSI | mmsi |
| BaseDateTime | base_date_time |
| LAT | latitude |
| LON | longitude |
| SOG | sog |
| COG | cog |
| Heading | heading |
| VesselName | vessel_name |
| IMO | imo |
| CallSign | call_sign |
| VesselType | vessel_type |
| Status | status |
| Length | length |
| Width | width |
| Draft | draft |
| Cargo | cargo |
| TransceiverClass | transceiver |
