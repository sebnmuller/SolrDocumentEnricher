package com.muller.solr.update.processors;

import java.io.IOException;

import com.muller.solr.update.processors.utilities.Utilities;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecursiveMergeExistingDoc extends UpdateRequestProcessor {

	private static final Logger log = LoggerFactory.getLogger(RecursiveMergeExistingDoc.class);

	private String delimiter;
	private String foreignIdField;
	private String localIdField;
	private NamedList<String> foreignSourceFieldList;
	private String solrUrl;
	private String requiredFieldKey;
	private String requiredFieldValue;

	public RecursiveMergeExistingDoc(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
		super(next);
	}

	@Override
	public void processAdd(AddUpdateCommand cmd) throws IOException {
		boolean processContinue = true;
		SolrInputDocument inputDoc = cmd.getSolrInputDocument();
		// Processor will only run if the input document contains localIdField as defined in the solrconfig.xml
		if (inputDoc.containsKey(localIdField)) {
			// If recursion break paremeters exist in the solrconfig then check the input document fulfills the condition
			if ((requiredFieldKey != null && requiredFieldValue != null)
					&& (!inputDoc.containsKey(requiredFieldKey) || !inputDoc.getFieldValue(requiredFieldKey).equals(requiredFieldValue))) {
				processContinue = false;
			}
			if (processContinue) {
				// If all conditions are satisfied, then we startup a Solr client
				RecursiveMergeExistingDocFactory.solrServer = Utilities.returnHttpSolrServer(solrUrl);
				// Create a copy of the input document in a format the utility expects and then retrieve all potential referenced documents
				SolrDocument rootDoc = Utilities.convertInputDocumentToDocument(inputDoc);
				Utilities.getChildren(inputDoc, rootDoc, localIdField, foreignSourceFieldList, foreignIdField, requiredFieldKey,
						requiredFieldValue);
				// Once we're done we don't need the Solr client anymore
				RecursiveMergeExistingDocFactory.solrServer.shutdown();
			}
		} else {
			log.debug("Document " + inputDoc.getFieldValue("id") + " does not contain " + localIdField);
		}
		super.processAdd(cmd);
	}

	public String getSolrUrl() {
		return solrUrl;
	}

	public void setSolrUrl(String solrUrl) {
		this.solrUrl = solrUrl;
	}

	public String getLocalIdField() {
		return localIdField;
	}

	public void setLocalIdField(String sourceField) {
		this.localIdField = sourceField;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public String getForeignIdField() {
		return foreignIdField;
	}

	public void setForeignIdField(String foreignKeyField) {
		this.foreignIdField = foreignKeyField;
	}

	public NamedList<String> getForeignSourceFieldList() {
		return foreignSourceFieldList;
	}

	public void setForeignSourceFieldList(NamedList<String> foreignSourceFieldList) {
		this.foreignSourceFieldList = foreignSourceFieldList;
	}

	public String getRequiredFieldKey() {
		return requiredFieldKey;
	}

	public void setRequiredFieldKey(String requiredFieldKey) {
		this.requiredFieldKey = requiredFieldKey;
	}

	public String getRequiredFieldValue() {
		return requiredFieldValue;
	}

	public void setRequiredFieldValue(String requiredFieldValue) {
		this.requiredFieldValue = requiredFieldValue;
	}

}
