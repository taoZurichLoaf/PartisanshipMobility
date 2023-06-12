# %%
# import packages
import numpy as np
import pandas as pd

# class to calculate state selfflow, inflow, outflow, totalflow

class Flows:

  # property is a dataframe
  def __init__ (self, data_frame):
    self.data_frame = data_frame

  # calculate selfflow (when origin == destination)
  def selfflows(self):
    # origin == destination
    selfflows = self.data_frame[self.data_frame.geoid_o == self.data_frame.geoid_d]
    # rename 
    selfflows = selfflows.rename(columns = {"visitor_flows": "visitor_selfflows", "pop_flows": "pop_selfflows"})
    # return the dataframe
    return selfflows

  # calculate outflow (when origin is fixed)
  def outflows(self):
    # discard the rows when origin == destination
    df_tmp = self.data_frame[self.data_frame.geoid_o != self.data_frame.geoid_d]
    # group by origins -> aggregate the flows
    outflows = df_tmp.groupby(by = "geoid_o").sum().reset_index()
    # rename 
    outflows = outflows.rename(columns = {"visitor_flows": "visitor_outflows", "pop_flows": "pop_outflows"})
    # discard unrelated columns
    outflows = outflows[["geoid_o", "visitor_outflows", "pop_outflows"]]
    # return the dataframe
    return outflows

  # calculate outflow (when destination is fixed)
  def inflows(self):
    # discard the rows when origin == destination
    df_tmp = self.data_frame[self.data_frame.geoid_o != self.data_frame.geoid_d]
    # group by destinations -> aggregate the flows
    inflows = df_tmp.groupby(by = "geoid_d").sum().reset_index()
    # rename
    inflows = inflows.rename(columns = {"visitor_flows": "visitor_inflows", "pop_flows": "pop_inflows"})
    # discard unrelated columns
    inflows = inflows[["geoid_d", "visitor_inflows", "pop_inflows"]]
    # return the dataframe
    return inflows

  def totalflows(self):
    # selfflows
    selfflows = self.selfflows()
    # outflows
    outflows = self.outflows()
    # inflows
    inflows = self.inflows() 
    # merge the three dataframes above
    totalflows = selfflows.merge(outflows, on ='geoid_o').merge(inflows, on ='geoid_d')
    # discard duplicated columnes
    totalflows = totalflows[["geoid_o", "date", "lat_o", "lng_o", "visitor_selfflows", "pop_selfflows", "visitor_inflows", "pop_inflows", "visitor_outflows", "pop_outflows"]]
    # rename
    totalflows = totalflows.rename(columns = {"geoid_o": "geoid", "lng_o": "lng", "lat_o": "lat"})
    # aggregate self-, out-, flows
    # Not include inflows for the two reasons: 1. to avoid duplication; 2. it relfects how many people left their homes.
    totalflows['visitor_totalflows'] = totalflows.visitor_selfflows + totalflows.visitor_outflows
    totalflows['pop_totalflows'] = totalflows.pop_selfflows + totalflows.pop_outflows
    # return the dataframe
    return totalflows



