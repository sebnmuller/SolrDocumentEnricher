package com.muller.solr.update.processors.utilities;

import java.util.Map.Entry;

import com.muller.solr.update.processors.RecursiveMergeExistingDocFactory;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utilities {

	private static final Logger log = LoggerFactory.getLogger(Utilities.class);

	// Retrieves all referenced documents, recursively if necessary and so long as recursion is allowed
	public static void getChildren(SolrInputDocument inputDoc, SolrDocument parentDocument, String childReferenceField,
			NamedList<String> foreignSourceFieldList, String childIdField, String requiredFieldKey, String requiredFieldValue) {
		// Progress only if the document contains references to existing documents and recursion is permitted to continue
		if (doesNodeHaveChildren(parentDocument, childReferenceField)
				&& continueRecursion(parentDocument, requiredFieldKey, requiredFieldValue)) {
			SolrDocumentList children = new SolrDocumentList();
			// Check that the incoming field contains content that is NOT null
			if (parentDocument.getFieldValue(childReferenceField) != null) {
				String childIdFieldValue = parentDocument.getFieldValue(childReferenceField).toString();
				String[] childrenIdValues = childIdFieldValue.split(RecursiveMergeExistingDocFactory.delimiter);
				// Created the list of referenced documents
				children.addAll(findChildren(childrenIdValues, childIdField));
				// Process the list and continue traversing the tree
				for (SolrDocument child : children) {
					getChildren(inputDoc, child, childReferenceField, foreignSourceFieldList, childIdField, requiredFieldKey,
							requiredFieldValue);
				}
			}
		} else {
			// When we reach a leaf node in the tree, retrieve field values as defined in the foreignSourceFieldList and add them to the
			// original input document
			for (Entry<String, String> foreignSourceFieldListEntry : foreignSourceFieldList) {
				String contentField = foreignSourceFieldListEntry.getKey();
				String destinationField = foreignSourceFieldListEntry.getValue();
				if (parentDocument.containsKey(contentField)) {
					addForeignFieldToInputDoc(inputDoc, parentDocument, destinationField, contentField);
					//TODO:Add child/leaf id to input doc
				}
			}
		}
	}

	// Determines if a document potentially has referenced documents based on the presence of a field that contains said references
	private static boolean doesNodeHaveChildren(SolrDocument parentDocument, String childReferenceField) {
		if (parentDocument.containsKey(childReferenceField)) {
			return true;
		}
		return false;
	}

	// Determines if the recursion process should end or not based on whether the current document contains a required field and the
	// required field contains a specific value
	private static boolean continueRecursion(SolrDocument parentDocument, String fieldKey, String fieldValue) {
		if (fieldKey != null && fieldValue != null) {
			if (parentDocument.containsKey(fieldKey) && parentDocument.getFieldValue(fieldKey).equals(fieldValue)) {
				return true;
			} else {
				return false;
			}
		}
		return true;
	}

	// Returns all documents whose childReferenceField's values match those in the childrenIdValues list
	private static SolrDocumentList findChildren(String[] childrenIdValues, String childReferenceField) {
		SolrDocumentList children = new SolrDocumentList();
		for (String childIdValue : childrenIdValues) {
			SolrDocument childDoc = getSolrDocumentFromIndex(childReferenceField, childIdValue, RecursiveMergeExistingDocFactory.solrServer);
			if (childDoc != null) {
				children.add(childDoc);
			} else {
				log.info("Child document with " + childReferenceField + ":" + childIdValue + " does not exist");
			}
		}
		return children;
	}

	// Retrieves a Solr document from an index based on a predefined id field and field value
	public static SolrDocument getSolrDocumentFromIndex(String foreignIdField, String foreignIdValue, HttpSolrServer solrServer) {
		SolrDocument foreignDocument = null;
		SolrQuery q = new SolrQuery(foreignIdField + ":" + foreignIdValue);
		QueryResponse rsp;
		try {
			rsp = solrServer.query(q);
			if (rsp != null) {
				if (!rsp.getResults().isEmpty()) {
					foreignDocument = rsp.getResults().get(0);
				}
			}
		} catch (SolrServerException e) {
			log.error("Solr server not reachable due to: " + e);
			e.printStackTrace();
		}
		return foreignDocument;
	}

	// Returns a Solr client we will use to carry out programmatic queries
	public static HttpSolrServer returnHttpSolrServer(String solrUrl) {
		HttpSolrServer solrServer = new HttpSolrServer(solrUrl);
		solrServer.setConnectionTimeout(5);
		return solrServer;
	}

	// Adds a single field to a SolrInputDocument
	public static SolrInputDocument addForeignFieldToInputDoc(SolrInputDocument inputDoc, SolrDocument foreignDocument,
			String destinationField, String foreignSourceField) {
		if (foreignDocument.getFieldNames().contains(foreignSourceField)) {
			String foreignSourceFieldValue = foreignDocument.getFieldValue(foreignSourceField).toString();
			inputDoc.addField(destinationField, foreignSourceFieldValue);
			//TODO: Add leaf doc's ID to a field in the inputDoc
			System.out.println("Adding foreignid_s " + foreignDocument.getFieldValue("id"));
			log.error("\n\n\n\n\nAdding foreignid_s " + foreignDocument.getFieldValue("id") + "\n\n\n\n\n");
			inputDoc.addField("foreignId_s", foreignDocument.getFieldValue("id"));
			return inputDoc;
		}
		log.info("Source field " + foreignSourceField + " does not exist in the source document. No changes made to parent document.");
		return inputDoc;
	}

	// Adds fields from a list to a Solr InputDocument
	public static SolrInputDocument addMultipleForeignFields(SolrInputDocument inputDocument, SolrDocument foreignDocument,
			NamedList<String> foreignSourceFieldList) {
		for (Entry<String, String> foreignSourceFieldListEntry : foreignSourceFieldList) {
			inputDocument = addForeignFieldToInputDoc(inputDocument, foreignDocument, foreignSourceFieldListEntry.getKey(),
					foreignSourceFieldListEntry.getValue());
		}
		return inputDocument;
	}

	// Rertieves a Solr config object from a parameter list
	public static Object getSolrConfigObjectFromParamList(NamedList<String> solrConfigParamList, String solrConfigParamName) {
		Object solrConfigVariable = null;
		Object solrConfigParam = getSolrConfigParamObject(solrConfigParamList, solrConfigParamName);
		if (solrConfigParam != null) {
			solrConfigVariable = solrConfigParam;
			return solrConfigVariable;
		}
		return solrConfigVariable;
	}

	@SuppressWarnings("unchecked")
	// Retrieves a Solr config named list as defined in the solrconfig.xml
	public static NamedList<String> getSolrConfigParamListFromParamList(NamedList<String> solrConfigParamList, String solrConfigParamName) {
		NamedList<String> solrConfigVariableList;
		Object solrConfigParam = getSolrConfigParamObject(solrConfigParamList, solrConfigParamName);
		if (solrConfigParam != null) {
			solrConfigVariableList = (NamedList<String>) solrConfigParam;
			return solrConfigVariableList;
		}
		return null;
	}

	// Retrieves a Solr config object as defined in the solrconfig.xml
	private static Object getSolrConfigParamObject(NamedList<String> solrConfigParamList, String solrConfigParamName) {
		if (solrConfigParamList.get(solrConfigParamName) != null) {
			Object solrConfigParam = solrConfigParamList.remove(solrConfigParamName);
			return solrConfigParam;
		}
		throw new SolrException(ErrorCode.SERVER_ERROR, solrConfigParamName + " must be defined.");
	}

	// Converts a Solr document from SolrInputDocument to SolrDocument format
	public static SolrDocument convertInputDocumentToDocument(SolrInputDocument inputDoc) {
		SolrDocument doc = new SolrDocument();
		for (String fieldName : inputDoc.getFieldNames()) {
			doc.setField(fieldName, inputDoc.getFieldValue(fieldName));
		}
		return doc;
	}

	// Converts a Solr document from SolrDocument to SolrInputDocument format
	public static SolrInputDocument convertDocumentToInputDocument(SolrDocument solrDoc) {
		SolrInputDocument inputDoc = new SolrInputDocument();
		for (String fieldName : solrDoc.getFieldNames()) {
			inputDoc.setField(fieldName, solrDoc.getFieldValue(fieldName));
		}
		return inputDoc;
	}

}
