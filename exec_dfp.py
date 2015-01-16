#!/usr/bin/python

"""This code is a generic script to get the DFP related Dimensions into DWH.
   Make sure PYTHONPATH is set before executing this script"""

import os
import sys
import argparse
import datetime
import codecs
import ConfigParser

# Import appropriate classes from the client library.
from googleads import dfp 
from googleads.dfp import DfpClient

parser = argparse.ArgumentParser(description='Generic script for DFP data download.')
parser.add_argument('-a','--api_name', help='This is the api name. The api\'s should be one of the following: ratecard, lica, product, proposal, customtargetingkeysandvalues, advertiser, order, label, user, customfield, adunit, adunitsize, lineitem, creative',required=True)
parser.add_argument('-s','--start_time', help='This is the start datetime for incremental pull, for example: 2014-06-01T00:00:00',required=False)
parser.add_argument('-e','--end_time', help='This is the end datetime for incremental pull, for example: 2014-06-01T00:00:00',required=False)
args = parser.parse_args()
print ('api name: %s' % args.api_name)

processDir = 'processing/'

def get_args(name):
  """ Function to derive start_time and end_time. If command line has start_time and end_time, it uses that. Otherwise, it derives from the config file dfp_api.param.
  For debug purposes or for historical load, one can directly pass the start_time and end_time through command line. Format for timestamp is YYYY-MM-DDTHH:MI:SS """
  config = ConfigParser.ConfigParser()
  config.read('dfp_api.param')
  #print len(sys.argv)
  if len(sys.argv) == 7:
    start_time = sys.argv[4]
    end_time = sys.argv[6]
  else:
    start_time = config.get(name,'start_time')
    end_time = config.get(name,'end_time')
  print 'Executing Api between start_time: %s and end_time: %s' % (start_time,end_time)
  return start_time,end_time

def main(client, name):
  print client, name
  if name == 'ratecard':
    get_ratecard(dfp_client)
  if name == 'lica':
    get_lica(dfp_client)
  if name == 'product':
    get_products(dfp_client)
  if name == 'proposal':
    get_proposals(dfp_client)
  if name == 'customtargetingkeysandvalues':
    get_customtargeting_keys_and_values(dfp_client)
  if name == 'advertiser':
    get_advertiser(dfp_client)
  if name == 'order':
    get_order(dfp_client)
  if name == 'label':
    get_label(dfp_client)
  if name == 'user':
    get_user(dfp_client)
  if name == 'customfield':
    get_custom_fields(dfp_client)
  if name == 'adunit':
    get_ad_unit(dfp_client)
  if name == 'adunitsize':
    get_ad_unitsize(dfp_client)
  if name == 'lineitem':
    get_line_item(dfp_client)
  if name == 'creative':
    get_creative(dfp_client)

def _ConvertDateFormat(date_time_value):
  """Converts Dict of date to datetime format suitable for database"""
  if date_time_value is None or date_time_value == '':
    date_time_obj = ''
  else:
    date_time_obj = datetime.datetime(int(date_time_value['date']['year']),
                                      int(date_time_value['date']['month']),
                                      int(date_time_value['date']['day']),
                                      int(date_time_value['hour']),
                                      int(date_time_value['minute']),
                                      int(date_time_value['second']))
  return date_time_obj

def get_ratecard(client):
  print "executing get_ratecard()"
  rate_card_service = client.GetService('RateCardService', version='v201408')
  statement = dfp.FilterStatement()
  rate_card_file = open(processDir+"ff_xsm_ratecard.out","wb+")
  while True:
    response = rate_card_service.getRateCardsByStatement(statement.ToStatement())
    #print response
    if 'results' in response:
      for rate in response['results']:
        data = "%s%s%s%s%s\n" % (rate["id"], rate["name"], rate["currencyCode"], rate["status"], _ConvertDateFormat(rate["lastModifiedDateTime"]))
        #print data
        rate_card_file.write(data)
      statement.offset += dfp.SUGGESTED_PAGE_LIMIT
    else:
      rate_card_file.close()
      print '\nNumber of results found: %s' % response['totalResultSetSize']
      sys.exit(0)
  else:
    print 'ERROR: Failed to execute get_ratecard()'

def get_products(client):
  print "executing get_products()"
  product_service = client.GetService('ProductService', version='v201405')
  statement = dfp.FilterStatement('WHERE lastModifiedDateTime > \'2014-07-14T00:00:00\'')
  while True:
    response = product_service.getProductsByStatement(statement.ToStatement())
    if 'results' in response:
      products = response['results']
      for product in products:
        print product 
      statement.offset += dfp.SUGGESTED_PAGE_LIMIT
  else:
    print 'ERROR: Failed to execute get_products()'
    sys.exit(1)

def get_proposals(client):
  print "executing get_proposals()"
  proposal_service = client.GetService('ProposalService', version='v201405')
  #statement = dfp.FilterStatement('WHERE lastModifiedDateTime > \'2014-07-14T00:00:00\'')
  statement = dfp.FilterStatement('WHERE 1=1')
  while True:
    response = proposal_service.getProposalsByStatement(statement.ToStatement())
    if 'results' in response:
      proposals = response['results']
      for proposal in proposals:
        print proposal
      statement.offset += dfp.SUGGESTED_PAGE_LIMIT
    else:
      break
  else:
    print 'ERROR: Failed to execute get_proposals()'
    sys.exit(1)
   
def get_customtargeting_keys_and_values(client):
  print "executing get_customtargeting_keys_and_values()"
  custom_targeting_service = client.GetService('CustomTargetingService', version='v201408') 
  f_customkeys = codecs.open(processDir+'ff_xfp_ad_key_lkp.out','wb+',encoding='utf-8')
  f_customvalues = codecs.open(processDir+'ff_xfp_ad_value_lkp.out','wb+',encoding='utf-8')

  #---- process to get custom targeting keys
  print "executing targeting keys .."
  targeting_key_statement = dfp.FilterStatement()
  while True:
    response = custom_targeting_service.getCustomTargetingKeysByStatement(targeting_key_statement.ToStatement())
    if 'results' in response: 
      for key in response['results']:
        #print key
	if key['displayName'] is None:
	  key['displayName'] = ''
        col_list = ('%s%s%s%s\n') % (key['id'],key['name'],key['displayName'],key['type'])
        f_customkeys.write(col_list)
      targeting_key_statement.offset += dfp.SUGGESTED_PAGE_LIMIT
    else:
      break
  else:
    print 'ERROR: Failed to execute get_customtargeting_keys_and_values() - custom targeting keys'
    sys.exit(1)
  f_customkeys.close()
  print '\nNumber of results found: %s' % response['totalResultSetSize']

  #---- process to get custom targeting values
  #---- This is the only process that doesnt use dfp and instead use the FilterStatement() instead of dfp.FilterStatement()
  #---- This is because, the process takes lot of time fetching >200k rows and hence we use a large page_size for this process
  print "executing targeting values .."
  i=0
  targeting_value_statement = FilterStatement()
  PAGE_LIMIT = 50000
  print 'using PAGE_LIMIT %s' % PAGE_LIMIT
  while True:
    response = custom_targeting_service.getCustomTargetingValuesByStatement(targeting_value_statement.ToStatement())
    #print response['totalResultSetSize']
    if 'results' in response:
      #print response['results']
      for value in response['results']:
  	if value['displayName'] is None:
          value['displayName'] = ''
        if value['name'] is None:
            value['name'] = ''
        col_list = ('%s%s%s%s\n') % (value['id'],value['name'],value['displayName'],value['matchType'])
        f_customvalues.write(col_list)
      targeting_value_statement.offset += 50000
      i = i+1
      print 'Downloaded pages %s' % i
    else:
      break
  else:
    print 'ERROR: Failed to execute get_customtargeting_keys_and_values() - custom targeting values'
    sys.exit(1)
  f_customvalues.close()
  print '\nNumber of results found: %s' % response['totalResultSetSize']

def get_creative(client):
  print "executing get_creative()"
  start_time,end_time = get_args('getCreativesByStatement')
  creative_service = client.GetService('CreativeService', version='v201408')
  f = codecs.open(processDir+'ff_xfp_ad_creative.out','wb+',encoding='utf-8')
  creative_custom_file = open(processDir+'ff_xfp_ad_creative_custom_fields.out','wb+')
  query = 'WHERE lastModifiedDateTime > %s and lastModifiedDateTime <= %s' % (start_time, end_time)
  statement = dfp.FilterStatement(query)
  while True:
    response = creative_service.getCreativesByStatement(statement.ToStatement())
    if 'results' in response:
      for creative in response['results']:
        if 'previewUrl' in creative:
	  previewUrl = creative['previewUrl']
	else:
	  previewUrl = ''
	if 'destinationUrl' in creative:
	  if creative['destinationUrl'] is None:
	    destinationUrl = ''
	  else:
	    destinationUrl = creative['destinationUrl']
	else:
	  destinationUrl = ''
        col_list = ('%s%s%s%sx%s%s%s%s%s\n') % (creative['id'],creative['advertiserId'],creative['name'],creative['size']['width'],creative['size']['height'],previewUrl,creative['Creative.Type'],_ConvertDateFormat(creative['lastModifiedDateTime']),destinationUrl)
        f.write(col_list)

	#---- process to write custom fields

        if 'customFieldValues' in creative:
	  for customFields in creative['customFieldValues']:
	    customfield_list=('%s%s%s\n') % (creative['id'],customFields['customFieldId'],customFields['customFieldOptionId'])
	    creative_custom_file.write(customfield_list)

      statement.offset += dfp.SUGGESTED_PAGE_LIMIT
    else:
      break
  else:
    print 'ERROR: Failed to execute get_creative()'
    sys.exit(1)
  f.close()
  print '\nNumber of results found: %s' % response['totalResultSetSize']
  

def get_line_item(client):
  print "executing get_line_item()"
  start_time,end_time = get_args('getLineItemsByStatement')
  line_item_service = client.GetService('LineItemService', version='v201408')
  f = codecs.open(processDir+'ff_xfp_ad.out','wb+',encoding='utf-8')
  fcap_file = open(processDir+'ff_xfp_ad_fcap.out','wb+')
  cust_tgt_file = open(processDir+'ff_xfp_ad_keyval_child.out','wb+')
  query = 'WHERE lastModifiedDateTime > %s and lastModifiedDateTime <= %s and isArchived = False' % (start_time, end_time)
  #print query
  statement = dfp.FilterStatement(query)
  while True:
    response = line_item_service.getLineItemsByStatement(statement.ToStatement())
    if 'results' in response:
      parent_id = 0
      child_id = 0
      for line_item in response['results']:
        if line_item['externalId'] is None:
	  line_item['externalId'] = ''
	#print line_item

        if 'notes' in line_item:
	  notes = line_item['notes'].replace('\n',' ')
	else:
	  notes = ''
	if 'deliveryIndicator' in line_item:
  	  actualDeliveryPercentage = line_item['deliveryIndicator']['actualDeliveryPercentage']
	  expectedDeliveryPercentage = line_item['deliveryIndicator']['expectedDeliveryPercentage']
	else:
	  actualDeliveryPercentage = ''
	  expectedDeliveryPercentage= ''
        if 'stats' in line_item:
	  impressionsDelivered = line_item['stats']['impressionsDelivered']
	  clicksDelivered = line_item['stats']['clicksDelivered']
	else:
	  impressionsDelivered = ''
	  clicksDelivered = ''
        if 'endDateTime' in line_item:
          endDateTime = _ConvertDateFormat(line_item['endDateTime'])
        else:
          endDateTime = ''

	col_list=('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s\n') % (line_item['id'],line_item['orderId'],line_item['name'],line_item['externalId'],line_item['lineItemType'],line_item['costType'],line_item['costPerUnit']['microAmount'],line_item['contractedUnitsBought'],line_item['budget']['microAmount'],clicksDelivered,impressionsDelivered,line_item['status'],notes,_ConvertDateFormat(line_item['startDateTime']), endDateTime,_ConvertDateFormat(line_item['lastModifiedDateTime']),line_item['lastModifiedDateTime']['timeZoneID'],expectedDeliveryPercentage,actualDeliveryPercentage,line_item['priority'],_ConvertDateFormat(line_item['creationDateTime']),int(line_item['isMissingCreatives']))
        f.write(col_list)

	#----- Derive FCAP data

	if 'frequencyCaps' in line_item:
	  for fcap in line_item['frequencyCaps']:
	    fcap_col_list = ('%s%s%s%s\n') % (line_item['id'],fcap['maxImpressions'],fcap['numTimeUnits'],fcap['timeUnit'])
	    fcap_file.write(fcap_col_list)

	#----- Derive CustomTargeting data

	if 'targeting' in line_item:
          if 'customTargeting' in line_item['targeting']:
	    #print 'Looping inside custom targeting'
	    #print line_item['targeting']['customTargeting']
	    customtgt = line_item['targeting']['customTargeting']
  	    if 'children' in customtgt:
	      #parent_id = parent_id+1
	      #print 'print children'
    	      for c in customtgt['children']:
		parent_id = parent_id+1
		#print c
		for cc in c['children']:
	          #parent_id = parent_id+1
		  if 'valueIds' in cc:
		    for val in cc['valueIds']:
		      child_id = child_id+1
		      #cust_tgt_cols = ('%s%s%sCustomCriteria%s%s%s%s%s\n') % (parent_id, child_id,line_item['id'],customtgt['logicalOperator'],c['logicalOperator'],cc['keyId'],cc['operator'],val)
		      cust_tgt_cols = ('%s%s%sCustomCriteria%s%s%s\n') % (parent_id, child_id,line_item['id'],cc['keyId'],val,cc['operator'])
		      cust_tgt_file.write(cust_tgt_cols)

      statement.offset += dfp.SUGGESTED_PAGE_LIMIT
    else:
      break
  else:
    print 'ERROR: Failed to execute get_line_item()'
    sys.exit(1)
  f.close()
  fcap_file.close()
  cust_tgt_file.close()
  print '\nNumber of results found: %s' % response['totalResultSetSize']


def get_custom_fields(client):
  custom_field_service = client.GetService( 'CustomFieldService', version='v201408')
  f = open(processDir+'ff_xfp_ad_custom_values.out','wb+')
  statement = dfp.FilterStatement('WHERE 1=1')
  while True:
    response = custom_field_service.getCustomFieldsByStatement(statement.ToStatement())
    print response
    if 'results' in response:
      for custom_field in response['results']:
	if 'options' in custom_field:
	  for opt in custom_field['options']:
	    col_list=('%s%s%s%s%s\n') % (custom_field['name'],custom_field['entityType'],opt['id'],opt['customFieldId'],opt['displayName'])
	    f.write(col_list)
        else:
          col_list=('%s%s%s%s%s\n') % (custom_field['name'],custom_field['entityType'],custom_field['id'],custom_field['id'],custom_field['name'])
          f.write(col_list)
      statement.offset += dfp.SUGGESTED_PAGE_LIMIT
    else:
      break
  else:
    print 'Failed to execute get_custom_fields()'
    sys.exit(1)
  f.close()
  print '\nNumber of results found: %s' % response['totalResultSetSize'] 

def get_ad_unitsize(client):
  inventory_service = client.GetService('InventoryService', version='v201408')
  f = open(processDir+'ff_xfp_ad_unit_size.out','wb+')
  statement = dfp.FilterStatement('WHERE 1=1')
  response = inventory_service.getAdUnitSizesByStatement(statement.ToStatement())
  #print response
  if response:
    for sz in response:
      col_list=('%s%s%s\n') % (sz['size']['width'],sz['size']['height'],sz['fullDisplayString'])
      f.write(col_list)
  else:
    print "ERROR: No response from get_ad_unitsize()"
    sys.exit(1)
  f.close()
  #print '\nNumber of results found: %s' % response['totalResultSetSize']
"""
    response = inventory_service.getAdUnitSizesByStatement(statement.ToStatement())
    if response:
      for sz in response:
        print sz['size']['width'],sz['size']['height'],sz['fullDisplayString']
      statement.offset += dfp.SUGGESTED_PAGE_LIMIT
    else:
      break
  f.close()
  print '\nNumber of results found: %s' % response['totalResultSetSize']	
"""

def get_ad_unit(client):
  print 'Executing get_ad_unit()'
  start_time,end_time = get_args('getAdUnitsByStatement')
  inventory_service = client.GetService('InventoryService', version='v201408')
  f = open(processDir+'ff_xfp_ad_site.out','wb+')
  query = 'WHERE lastModifiedDateTime > %s and lastModifiedDateTime <= %s' % (start_time, end_time)
  statement = dfp.FilterStatement(query)
  while True:
    response = inventory_service.getAdUnitsByStatement(statement.ToStatement())
    if 'results' in response:
      for ad_unit in response['results']:
	if 'parentId' in ad_unit:
	  parentId = ad_unit['parentId']
	else:
	  parentId = ''
        col_list=('%s%s%s%s%s%s%s%s%s%s%s\n') % (ad_unit['id'],parentId,ad_unit['name'],ad_unit['description'],ad_unit['status'],ad_unit['adUnitCode'],ad_unit['inheritedAdSenseSettings']['value']['adType'],ad_unit['targetPlatform'],ad_unit['mobilePlatform'],int(ad_unit['explicitlyTargeted']),_ConvertDateFormat(ad_unit['lastModifiedDateTime']))
	f.write(col_list)
      statement.offset += dfp.SUGGESTED_PAGE_LIMIT
    else:
      break
  else:
    print 'ERROR: Failed to execute get_ad_unit() inventory service'
    sys.exit(1)
  f.close()
  print '\nNumber of results found: %s' % response['totalResultSetSize']

def get_label(client):
  print 'Executing get_label()'
  label_service = client.GetService('LabelService', version='v201408')
  f = open(processDir+'ff_xfp_ad_label.out','wb+')
  statement = dfp.FilterStatement('ORDER BY name')
  while True:
    response = label_service.getLabelsByStatement(statement.ToStatement())
    if 'results' in response:
      # Display results.
      for label in response['results']:
	# Convert True/False to boolean
        col_list = ('%s%s%s\n') % (label['id'],label['name'],int(label['isActive']))
	f.write(col_list)
      statement.offset += dfp.SUGGESTED_PAGE_LIMIT
    else:
      break  
  else:
    print 'ERROR: Failed to execute get_label()'
    sys.exit(1)
  f.close()
  print '\nNumber of results found: %s' % response['totalResultSetSize']


def get_user(client):
  """ Derive users from Dfp. This is a full data download instead of incremental load."""
  user_service = client.GetService('UserService', version='v201408')
  f = codecs.open(processDir+'ff_xfp_users.out','wb+',encoding='utf-8')
  query = "ORDER BY Id"
  #query = ""
  statement = dfp.FilterStatement(query)
  while True:
    response = user_service.getUsersByStatement(statement.ToStatement())
    #print response
    if 'results' in response:
      #print response['results']
      for user in response['results']:
        #print user['id']
        col_list = ('%s%s%s%s%s%s%s%s\n') % (user['id'],user['name'],user['email'],user['roleId'],user['roleName'],user['preferredLocale'],user['UserRecord.Type'],int(user['isActive']))
        f.write(col_list)
      #statement.IncreaseOffsetBy(500)
      statement.offset += dfp.SUGGESTED_PAGE_LIMIT
    else:
      break
  else:
    print 'ERROR: Failed to execute get_user()'
    sys.exit(1)
  f.close()
  print '\nNumber of results found: %s' % response['totalResultSetSize']

def get_order(client):
  """derive orders data from Dfp"""
  start_time,end_time = get_args('getOrdersByStatement')
  #print start_time,end_time
  order_service = client.GetService('OrderService', version='v201408')
  query = "WHERE lastModifiedDateTime > %s and lastModifiedDateTime <= %s" % (start_time, end_time)
  statement = dfp.FilterStatement(query)
  f = codecs.open(processDir+'ff_xfp_ad_order.out','wb+',encoding='utf-8')
  f_sec = open(processDir+'ff_xfp_ad_order_secondary_trafficker.out','wb+')
  #f_ss = open(processDir+'ff_ad_order_salesperson.out','wb+')
  while True:
    response = order_service.getOrdersByStatement(statement.ToStatement())
    if 'results' in response:
      orders = response['results']
      for order in orders:
        if 'salespersonId' in order:
          salespersonId = order['salespersonId']
        else:
          salespersonId = ''
        
	if 'endDateTime' in order:
	  endDateTime = _ConvertDateFormat(order['endDateTime'])
	else:
	  endDateTime = ''

        if 'totalBudget' in order:
	  totalBudget = order['totalBudget']['currencyCode']
	else:
	  totalBudget = ''

	col_list=('%s%s%s%s%s%s%s%s%s%s%s%s\n') % (order['advertiserId'],order['id'],order['name'],order['traffickerId'],salespersonId,order['totalClicksDelivered'],order['totalImpressionsDelivered'],_ConvertDateFormat(order['startDateTime']),endDateTime,_ConvertDateFormat(order['lastModifiedDateTime']),order['currencyCode'],order['status'])
        f.write(col_list)
	if 'secondaryTraffickerIds' in order:
          for i in order['secondaryTraffickerIds']:
            sectrf_col = ('%s%s\n') % (order['id'],i)
	    f_sec.write(sectrf_col)
        if 'secondarySalespersonIds' in order:
          for j in order['secondarySalespersonIds']:
            ss_col = ('%s%s\n') % (order['id'],j)
            f_sec.write(ss_col)
      statement.offset += dfp.SUGGESTED_PAGE_LIMIT
    else:
      break
  else:
    print 'ERROR: Failed to execute get_order()'
    sys.exit(1)
  f.close()
  f_sec.close()
  #f_ss.close()
  print '\nNumber of results found: %s' % response['totalResultSetSize']

def get_lica(client):
  print 'Executing get_lica()'
  start_time,end_time = get_args('LineItemCreativeAssociationService')
  lica_service = client.GetService('LineItemCreativeAssociationService', version='v201408')
  query = 'WHERE lastModifiedDateTime > %s and lastModifiedDateTime <= %s' % (start_time, end_time)
  statement = dfp.FilterStatement(query)
  f = open(processDir+'ff_xfp_ad_id_creative_association.out', 'wb+')
  while True:
    response = lica_service.getLineItemCreativeAssociationsByStatement(statement.ToStatement())
    if 'results' in response:
      for lica in response['results']:
        col_list=('%s%s%s%s\n') % (lica['lineItemId'],lica['creativeId'],lica['status'],_ConvertDateFormat(lica['lastModifiedDateTime']))
        f.write(col_list)
      statement.offset += dfp.SUGGESTED_PAGE_LIMIT
    else:
      break
  else:
    print 'ERROR: Failed to execute get_lica()'
    sys.exit(1)
  f.close()
  print '\nNumber of results found: %s' % response['totalResultSetSize']   
  

def get_advertiser(client):
  # Initialize appropriate service.
  start_time,end_time = get_args('getCompaniesByStatement')
  company_service = client.GetService('CompanyService', version='v201408')
  query = 'WHERE lastModifiedDateTime > %s and lastModifiedDateTime <= %s' % (start_time, end_time)
  statement = dfp.FilterStatement(query)
  f = codecs.open(processDir+'ff_xfp_advertiser.out', 'wb+',encoding='utf-8')
  # Get companies by statement.
  while True:
    response = company_service.getCompaniesByStatement(statement.ToStatement())
    if 'results' in response:
      # Display results.
      for company in response['results']:
	if 'externalId' in company:
	  externalId = company['externalId']
	else:
	  externalId = ''
	## There are multiple labels for a company
        if 'appliedLabels' in company:
          for i in company['appliedLabels']:
	    col_list=('%s%s%s%s%s%s\n') % (company['id'],company['name'],i['labelId'],externalId,company['type'],_ConvertDateFormat(company['lastModifiedDateTime']))
	    f.write(col_list)
        else:
          col_list=('%s%s%s%s%s%s\n') % (company['id'],company['name'],'',externalId,company['type'],_ConvertDateFormat(company['lastModifiedDateTime']))
          f.write(col_list)
	  #print col_list
      statement.offset += dfp.SUGGESTED_PAGE_LIMIT
    else:
      break
  else:
    print 'ERROR: Failed to execute get_advertiser()'
    sys.exit(1)
  f.close()
  print '\nNumber of results found: %s' % response['totalResultSetSize']

class FilterStatement(object):
  """A statement object for PQL and get*ByStatement queries.

  The FilterStatement object allows for user control of limit/offset. It
  automatically limits queries to the suggested page limit if not explicitly
  set.
  """

  def __init__(
    self, where_clause='', values=None, limit=50000, offset=0):
    self.where_clause = where_clause
    self.values = values
    self.limit = limit
    self.offset = offset

  def _GetOffset(self):
    return self._offset

  def _SetOffset(self, value):
    self._offset = value

  offset = property(_GetOffset, _SetOffset)

  def _GetLimit(self):
    return self._limit

  def _SetLimit(self, value):
    self._limit = value

  limit = property(_GetLimit, _SetLimit)

  def IncreaseOffsetBy(self, increase_offset):
    self.offset += increase_offset

  def ToStatement(self):
    return {'query': '%s LIMIT %d OFFSET %d' %
                     (self.where_clause, self._limit, self._offset),
            'values': self.values}
  

if __name__ == '__main__':
  # Initialize client object.
  dfp_client = dfp.DfpClient.LoadFromStorage('googleads.yaml')
  a=args.api_name
  main(dfp_client, a)
