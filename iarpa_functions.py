#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API function calls (from IARPA)
Created on Wed Mar 21 14:24:16 2018
@author: tiffany
"""
import requests
import time

class GfcApi(object):
    """
        An example class for interacting with the IARPA Geopolitical Forecasting Challenge
        API.  Note that this code is for reference purposes, no warranties are expressed
        or implied.  
    """
    def __init__(self,token,server,proxy=None,verbose=False):
        """
            Create an instance of an API client. This assumes you have an OAuth token.
            
            Arguments
            
            REQUIRED
            token - <string> - The secret API token assigned when registering on the 
                               Cultivate platform
            
            server - <string> - The beginning of the server url in the form:
                                https://api.XXXXXXX.com.  This is described in the
                                Cultivate API documentation
            
            OPTIONAL
            proxy - <dictionary> - If you are behind a proxy server, you can specify the details
                                   in the form: 
                                       proxy = {'http': http_proxy,
                                                'https': https_proxy,
                                                'ftp': ftp_proxy}
                                   where an individual entry might be [ip address:port]. See the
                                   requests library documentation for more details.
                Default: None
                
            verbose - <boolean> - If true, we print GET and POST request URLs and params
                Default: False
            
        """
        
        self.token = token
        self.server = server
        self.proxy = proxy
        self.verbose = verbose
        
        self.sess = requests.session()
        self.rate_limit_delay = 1 #seconds between subsequent API calls
        self.last_call_time = 0.0 
        self.set_urls()
    
    def set_urls(self):
        if not self.server.endswith('/'):
            self.server += '/'
    
        self.api_base = self.server + 'api/v1/'
    
        self.consensus_histories_url = self.server + 'aggregation/api/v1/control/consensus_histories'
        self.external_prediction_sets_url = self.api_base + 'external_prediction_sets'
        self.prediction_sets_url = self.api_base + 'control/prediction_sets'
        self.questions_url = self.api_base + 'questions'

    def get_questions(self, status=None, created_before=None, created_after=None,
                  sort='published_at', updated_before=None, updated_after=None):
        """
            This function retrieves Individual Forecasting Problems (IFPs).

            Optional Inputs:
            status - <string> - IFP status
                    Possible Values:
                        'active' - only return questions that are currently open for forecasting
                        'closed' - return all resolved or otherwised closed questions
                        'all'    - return all active and closed questions
                    Default Value:
                        'active'

            created_before - <datetime> - returns only questions created before this time

            created_after - <datetime> - returns only questions created after this time
            
            sort - <string> - Sort order of returned questions
                    Possible Values:
                        'published_at'
                        'ends_at'
                        'resolved_at'
                        'prediction_sets_count'
                    Default Value:
                        'published_at'
            
            updated_before - <datetime> - returns only questions updated before this time
            
            updated_after - <datetime> - returns only questions updated after this time
                    
             Output:
            JSON representation of a list of Individual Forecasting Problems
        """
        
        url = self.questions_url
        section = 'questions'
        params={}
        
        if created_before:
            params['created_before'] = created_before.isoformat()
        if created_after:
            params['created_after'] = created_after.isoformat()
        if created_before:
            params['updated_before'] = updated_before.isoformat()
        if created_after:
            params['updated_after'] = updated_after.isoformat()
        if status:
            params['status'] = status
        if sort:
            params['sort'] = sort  
        
        return self._get_pages(url=url,section=section,params=params)
    
    def get_human_forecasts(self, question_id=None, created_before=None, created_after=None,
                           updated_before=None, updated_after=None):

        """
            This function retrieves the stream of human forecasts against IFPs.

            Optional Inputs:
            question_id - <integer> - returns predictions for a single question
                    Default Value:
                        None

            created_before - <datetime> - returns only predictions created before this time

            created_after - <datetime> - returns only predictions created after this time
            
            updated_before - <datetime> - returns only predictions updated before this time
            
            updated_after - <datetime> - returns only predictions updated after this time
                    
             Output:
            JSON representation of a list of human forecasts
        """
        
        url = self.prediction_sets_url
        section = 'prediction_sets'
        params={}
        
        if created_before:
            params['created_before'] = created_before.isoformat()
        if created_after:
            params['created_after'] = created_after.isoformat()
        if updated_before:
            params['updated_before'] = updated_before.isoformat()
        if updated_after:
            params['updated_after'] = updated_after.isoformat()
        if question_id:
            params['question_id'] = question_id
        
        return self._get_pages(url=url,section=section,params=params)        
    
    def get_consensus_histories(self, question_id=None, created_before=None, created_after=None,
                           updated_before=None, updated_after=None):

        """
            This function retrieves the consensus of human forecasts against IFPs.

            NOTE: You need to include some date constraints after your first use of this API. 
            Always utilize the created_after parameter to pull only those records that have 
            been created since you last accessed the API. Do not attempt to pull every 
            record/page of the history.

            Optional Inputs:
            
            question_id - <integer> - returns only predictions made about a specific IFP
                Default Value
                    None

            created_before - <datetime> - returns only predictions created before this time

            created_after - <datetime> - returns only predictions created after this time
            
            updated_before - <datetime> - returns only predictions updated before this time
            
            updated_after - <datetime> - returns only predictions updated after this time
                    
             Output:
            JSON representation of a list of human forecasts
        """
        
        if (not created_before) and (not created_after) and (not updated_before) and (not updated_after):
            print("After your first query, use a date constraint (created_before/after or",\
                  "updated_before/after) to get consensus history. Old values won't change")
        
        url = self.consensus_histories_url
        section = 'consensus_histories'
        params={}
        
        if question_id:
            params['question_id'] = str(question_id)
        if created_before:
            params['created_before'] = created_before.isoformat()
        if created_after:
            params['created_after'] = created_after.isoformat()
        if updated_before:
            params['updated_before'] = updated_before.isoformat()
        if updated_after:
            params['updated_after'] = updated_after.isoformat()
        
        return self._get_pages(url=url,section=section,params=params)   
    
    def submit_forecast(self,question_id,method_name,predictions):
        """
            Submit probabilistic forecasts against a question.
            
            Required Parameters
            
            question_id - <integer> - The question_id of the IFP being forecast against
            
            method_name - <string> - The name of one of your 25 forecasting methods. Up to 50 chars
                             NOTE: This is used to track and score your forecasting methods. You
                             are responsible for keeping track of your named methods. Using a new
                             method_name will automatically add a new method - unless you have
                             already created 25 methods. In that case, you'll get an error message
                             in the response.
                             
            predictions - <list> - A list of Dictionaries in the form .
                                                   {'answer_id': <Integer>, 'value': <Decimal>}
                            
                          If the question is binary (exactly two possible answers), you only submit a
                          prediction for one possible answer, with the other being equal to 1 minus
                          your prediction for option A.
                          
                          NOTE: The set of values in the forecast must equal exactly 1.0 or you will 
                          receive an error message in the response.
            
     RESPONSE
     The json response will either summarize your forecast to this question, or it will contain an 
     error message indicating why it wasn't accepted.  You are responsible for recieving and reviewing
     the response to ensure that your forecast was accepted, and reflects your intentions.  You can 
     resubmit forecasts to a particular IFP repeatedly over the course of a forecast day, and each
     new submission will replace older submissions for scoring purposes. Review the GF Challenge
     Rules for details on forecast submission and scoring.
        """
    
        url = self.external_prediction_sets_url
        
        params={'external_prediction_set':{'question_id':question_id,
                                          'external_predictor_attributes':
                                           {'method_name':method_name},
                                           'external_predictions_attributes':predictions}
                }
        
        return self._post(url,params)
    
    def _forecast_template(self,ifp):
        """
            A tiny little helper function to create the basis for the predictions parameter in
            the submit_forecast function.  You pass an IFP from the questions API into this 
            function and receive a list of 'answer_id' and 'value' dictionaries that are
            needed to submit a forecast.  
            
            NOTE: This uses the existing 'probability' value from the questions API which should be
            replaced with your own forecast values.
        """
        
        output = [{'answer_id':a['id'],'value':a['probability']} for a in ifp['answers']]
        return output
    
    def _get_pages(self,url,params,section):
        
        """
            This function uses _get to make authenticated calls to the
            relevant API endpoints with the user-provided parameters.
            
            This function handles paging through results, and returns only the list from
            the resulting json result(s).
            
            The 'url' and 'params' describe the API query, the 'section' is the key in the
            returned json that contains the list of query results (e.g., 'questions').
        """
        if self.verbose:
            print('Get Pages for {}'.format(url))
            print(params)
        page = 1
        maxPage = 1
        
        all_results = []
        this_batch = []
        while page <= maxPage: 
            
            params['page']=page
            resp = self._get(url=url,params=params)
            maxPage = int(resp.headers.get('X-Total-Page-Count',0))
            try:
                results=resp.json()
            except:
                results=None
            if isinstance(results,(list,dict)):
                if 'errors' in results:
                    print(results['errors'])
                    return results
                
                this_batch = results[section]
                all_results.extend(this_batch)

                page+=1
            else:
                if self.verbose:
                    print("PROBLEM")
                return results

        return all_results                
        
    def _get(self,url,params):
        """
            A helper function that handles authentication and rate limiting.
            
            Given a URL and a set of parameters, this function calls the Cultivate API
            and returns the json response.
        """
        
        while time.time() < self.last_call_time + self.rate_limit_delay:
            if self.verbose:
                print("{}: Sleeping".format(time.ctime()))
            time.sleep(1)
        
        headers={'Authorization':'Bearer ' + self.token} #This is needed to authenticate

        if self.verbose:
            print("{}: GETTING {}".format(time.ctime(),url))
            safeHeaders = {k:v for k,v in headers.items() if k!='Authorization'}
            safeHeaders['Authorization']="Bearer <shhhhhh it's a secret>"
            print("\tHeaders: {}".format(safeHeaders))
            print("\tArgs: {}".format(params))
        resp = self.sess.get(url, headers=headers, params=params, proxies=self.proxy)
                                                                                         
        self.last_call_time = time.time()
        return resp
    
    def _post(self,url,params):
        """
            A helper function that handles authentication.
            
            Given a URL and a set of parameters, this function submits a POST to the 
            Cultivate API and returns the json response.
            
            Output
            JSON response describing the forecast or indicating an error.

        """

        while time.time() < self.last_call_time + self.rate_limit_delay:
            if self.verbose:
                print("{}: Sleeping".format(time.ctime()))
            time.sleep(1)
        
        headers={'Authorization':'Bearer ' + self.token} #This is needed to authenticate

        if self.verbose:
            print("{}: POSTING {}".format(time.ctime(),url))
            safeHeaders = {k:v for k,v in headers.items() if k!='Authorization'}
            safeHeaders['Authorization']="Bearer <shhhhhh it's a secret>"
            print("\tHeaders: {}".format(safeHeaders))
            print("\tArgs: {}".format(params))
        resp = self.sess.post(url, headers=headers, json=params, proxies=self.proxy) 
                                                                                         
        self.last_call_time = time.time()
        
        return resp.json()
    
