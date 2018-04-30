from __future__ import absolute_import
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import argparse
import logging

def get_schema():
    schema= 'Event_date:DATE, Event_time:TIME, Time:STRING, userid:STRING, advertiserid:STRING, orderid:STRING, lineitemid:STRING, CreativeId:STRING, CreativeVersion:STRING, CreativeSize:STRING, AdUnitId:STRING, CustomTargeting:STRING, Domain:STRING, CountryId:STRING, Country:STRING, RegionId:STRING, Region:STRING, MetroId:STRING, Metro:STRING, CityId:STRING, City:STRING, PostalCodeId:STRING, PostalCode:STRING, BrowserId:STRING, Browser:STRING, OSId:STRING, OS:STRING, OSVersion:STRING, BandwidthId:STRING, BandWidth:STRING, TimeUsec:STRING, AudienceSegmentIds:STRING, Product:STRING, RequestedAdUnitSizes:STRING, BandwidthGroupId:STRING, MobileDevice:STRING, MobileCapability:STRING, MobileCarrier:STRING, IsCompanion:STRING, TargetedCustomCriteria:STRING, DeviceCategory:STRING, IsInterstitial:STRING, EventTimeUsec2:STRING, YieldGroupNames:STRING, YieldGroupCompanyId:STRING, MobileAppId:STRING, RequestLanguage:STRING, DealId:STRING, DealType:STRING, AdxAccountId:STRING, SellerReservePrice:STRING,  Buyer:STRING, Advertiser:STRING, Anonymous:STRING, ImpressionId:STRING'
    return schema

class ToTableRowDoFn(beam.DoFn):
  def process(self,line):
    values = line.split(',')
    row = {}
    timeval=values[0]
# if header row skip the line
    if (timeval=='Time'):
       return None
    row["event_date"] = timeval[0:10]
    row["event_time"] = timeval[11:20]
    row["time"] = timeval
    row["userid"] = values[1]
    row["advertiserid"] = values[2]
    row["orderid"] = values[3]
    row["lineitemid"] = values[4]
    row["CreativeId"]=values[5]
    row["CreativeVersion"]=values[6]
    row["CreativeSize"]=values[7]
    row["AdUnitId"]=values[8]
    row["CustomTargeting"]=values[9]
    row["Domain"]=values[10]
    row["CountryId"]=values[11]
    row["Country"]=values[12]
    row["RegionId"]=values[13]
    row["Region"]=values[14]
    row["MetroId"]=values[15]
    row["Metro"]=values[16]
    row["CityId"]=values[17]
    row["City"]=values[18]
    row["PostalCodeId"]=values[19]
    row["PostalCode"]=values[20]
    row["BrowserId"]=values[21]
    row["Browser"]=values[22]
    row["OSId"]=values[23]
    row["OS"]=values[24]
    row["OSVersion"]=values[25]
    row["BandwidthId"]=values[26]
    row["BandWidth"]=values[27]
    row["TimeUsec"]=values[28]
    row["AudienceSegmentIds"]=values[29]
    row["Product"]=values[30]
    row["RequestedAdUnitSizes"]=values[31]
    row["BandwidthGroupId"]=values[32]
    row["MobileDevice"]=values[33]
    row["MobileCapability"]=values[34]
    row["MobileCarrier"]=values[35]
    row["IsCompanion"]=values[36]
    row["TargetedCustomCriteria"]=values[37]
    row["DeviceCategory"]=values[38]
    row["IsInterstitial"]=values[39]
    row["EventTimeUsec2"]=values[40]
    row["YieldGroupNames"]=values[41]
    row["YieldGroupCompanyId"]=values[42]
    row["MobileAppId"]=values[43]
    row["RequestLanguage"]=values[44]
    row["DealId"]=values[45]
    row["DealType"]=values[46]
    row["AdxAccountId"]=values[47]
    row["SellerReservePrice"]=values[48]
    row["Buyer"]=values[49]
    row["Advertiser"]=values[50]
    row["Anonymous"]=values[51]
    row["ImpressionId"]=values[52]
    return [row]

''' This is to group elements by event date to generate dimensions 
    Emits A tuple of event date + dimension , 1 combination 
    Event_Date + City 
    Event Date + Browser  
    ....
'''
class CollectCities(beam.DoFn):
  def process(self,element):
      return [(element['event_date']+','+'CITY'+','+element['City'],1 )]

class CollectBrowser(beam.DoFn):
  def process(self,element):
      return [(element['event_date']+','+'BROWSER'+','+element['Browser'],1 )]
    
class CollectMetro(beam.DoFn):
  def process(self,element):
      return [(element['event_date']+','+'METRO'+','+element['Metro'],1 )]

class CollectOS(beam.DoFn):
  def process(self,element):
      return [(element['event_date']+','+'OS'+','+element['OS'],1 )]

class CollectPostalCode(beam.DoFn):
  def process(self,element):
      return [(element['event_date']+','+'POSTALCODE'+','+element['PostalCode'],1 )]

class CollectMobileDevices(beam.DoFn):
  def process(self,element):
      return [(element['event_date']+','+'MOBILEDEVICE'+','+element['MobileDevice'],1 )]

class CollectRegions(beam.DoFn):
  def process(self,element):
      return [(element['event_date']+','+'REGION'+','+element['Region'],1 )]


class BuildDimRecord(beam.DoFn):
    def process(self,element):
       event_date, dimension_type, dimension = element[0].split(',')
       value=element[1]
       row={}
       row['event_date']=event_date
       row['dimension_type']=dimension_type.strip()
       row['dimension'] = dimension.strip()
       row['aggr_value']=value
       return [row]

def run(argv=None):
    logging.info("started.........")
    dimension_schema = 'event_date:DATE, dimension_type:STRING, dimension:STRING, aggr_value:INTEGER'
    AGGR_TABLE='dwh_prod.impr_daily_aggr'
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--input',
                      dest='input',
                      default='gs://inbound_data/test.csv',
                      help='Input file to process.')
    parser.add_argument('--output',
                      dest='output',   
                      help='Output file to write results to.')
    bq_schema=get_schema()
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)
    
    rows= p | 'ReadFileFromGCS' >> ReadFromText(known_args.input) | 'TransformRows' >> beam.ParDo(ToTableRowDoFn()) 
    rows | 'WriteToBQ-Impressions' >> beam.io.WriteToBigQuery(
        known_args.output,
        schema=bq_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
   
    browser =  rows | beam.ParDo(CollectBrowser())|  "Grouping Browsers" >> beam.GroupByKey() |  "Calculating Impression Count By Browser" >> beam.CombineValues(beam.combiners.CountCombineFn()) | "Converting Browser Dim Rows" >> beam.ParDo(BuildDimRecord())
    browser | 'WriteToBQ-Dim-Browsers' >> beam.io.WriteToBigQuery(
    AGGR_TABLE,
    schema=dimension_schema,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
   

    cities =  rows | beam.ParDo(CollectCities())|  "Grouping Cities" >> beam.GroupByKey() |  "Calculating Impression Count By City" >> beam.CombineValues(beam.combiners.CountCombineFn()) | "Converting Cities Dim Rows" >>beam.ParDo(BuildDimRecord())
    cities | 'WriteToBQ-Dim-Cities' >> beam.io.WriteToBigQuery(
    AGGR_TABLE,
    schema=dimension_schema,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
       

    metros =  rows | beam.ParDo(CollectMetro())|  "Grouping Metros" >> beam.GroupByKey() |  "Calculating Impression Count By Metro" >> beam.CombineValues(beam.combiners.CountCombineFn()) | "Converting Metro Dim Rows" >>beam.ParDo(BuildDimRecord())
    metros | 'WriteToBQ-Dim-Metro' >> beam.io.WriteToBigQuery(
    AGGR_TABLE,
    schema=dimension_schema,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    
    oss =  rows | beam.ParDo(CollectOS())|  "Grouping OS" >> beam.GroupByKey() |  "Calculating Impression Count By OS" >> beam.CombineValues(beam.combiners.CountCombineFn()) | "Converting OS Dim Rows" >>beam.ParDo(BuildDimRecord())
    oss | 'WriteToBQ-Dim-OS' >> beam.io.WriteToBigQuery(
    AGGR_TABLE,
    schema=dimension_schema,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
   
    PostalCodes =  rows | beam.ParDo(CollectPostalCode())|  "Grouping PostalCodes" >> beam.GroupByKey() |  "Calculating Impression Count By PostalCode" >> beam.CombineValues(beam.combiners.CountCombineFn()) | "Converting PostalCode Dim Rows" >>beam.ParDo(BuildDimRecord())
    PostalCodes | 'WriteToBQ-Dim-PostalCodeOS' >> beam.io.WriteToBigQuery(
    AGGR_TABLE,
    schema=dimension_schema,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
   
    Moobiledevices =  rows | beam.ParDo(CollectMobileDevices())|  "Grouping Devices" >> beam.GroupByKey() |  "Calculating Impression Count By Device" >> beam.CombineValues(beam.combiners.CountCombineFn()) | "Converting Device Dim Rows" >>beam.ParDo(BuildDimRecord())
    Moobiledevices | 'WriteToBQ-Dim-MobileDevice' >> beam.io.WriteToBigQuery(
    AGGR_TABLE,
    schema=dimension_schema,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

    Regions =  rows | beam.ParDo(CollectRegions())|  "Grouping Regions" >> beam.GroupByKey() |  "Calculating Impression Count By Region" >> beam.CombineValues(beam.combiners.CountCombineFn()) | "Converting Region Dim Rows" >>beam.ParDo(BuildDimRecord())
    Regions | 'WriteToBQ-Dim-Region' >> beam.io.WriteToBigQuery(
    AGGR_TABLE,
    schema=dimension_schema,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
