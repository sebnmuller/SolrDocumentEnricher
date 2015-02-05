package com.muller.solr.update.processors;

import com.muller.solr.update.processors.utilities.Utilities;

import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;

/**
 *
 * <p>
 * Retrieves an existing document field or list of fields based on a <code>localIdField</code> in
 * an input document and adds them as new fields to it. The <code>localIdField</code> can contain
 * a single value or a delimited list of IDs, split by the <code>delimiter</code> parameter.
 * The fields to copy and their mappings are defined in the <code>foreignSourceFieldList</code>.
 * The entry keys are used as the new fields' key and the entry values as the new fields' value.
 * 
 * If the referenced document also contains <code>localIdField</code> with content the processor
 * will recursively retrieve documents until it finds the child document with no more references 
 * and actual field content. Recursion can be broken using the <code>requiredFieldKey</code> and
 * <code>requiredFieldValue</code> parameters. They only allow ongoing traversal if the currently
 * processed document contains a field as defined by the parameters.
 *
 * The referenced document(s) can exist in a local or remote Solr index. Default Solr URL is 
 * http://localhost:8983/solr when not defined.
 *
 * <pre class="prettyprint">
 *  &lt;processor class="com.muller.solr.update.processors.RecursiveMergeExistingDocFactory" &gt;
 *       &lt;str name="localIdField"&gt;parent_s&lt;/str&gt;
 *      &lt;str name="foreignIdField"&gt;pageid&lt;/str&gt;
 *     &lt;lst name="foreignSourceFieldList"&gt;
 *      &lt;str name="title"&gt;test_ss&lt;/str&gt;
 *    &lt;/lst&gt;
 *   &lt;str name="requiredFieldKey"&gt;pagetypename&lt;/str&gt;
 *       &lt;str name="requiredFieldValue"&gt;TemaPage&lt;/str&gt;
 *  &lt;/processor&gt;
 * </pre>
 *
 */

public class RecursiveMergeExistingDocFactory extends UpdateRequestProcessorFactory {

	private static final String DELIMITER_PARAM = "delimiter";
	private static final String LOCAL_ID_FIELD = "localIdField";
	private static final String FOREIGN_ID_FIELD = "foreignIdField";
	private static final String FOREIGN_SOURCE_FIELD_LIST = "foreignSourceFieldList";
	private static final String SOLR_URL = "solrUrl";
	private static final String REQUIRED_FIELD_KEY = "requiredFieldKey";
	private static final String REQUIRED_FIELD_VALUE = "requiredFieldValue";

	public static String delimiter = ";";
	private String foreignIdField;
	private String localIdField;
	private NamedList<String> foreignSourceFieldList;
	private String solrUrl = "http://localhost:8983/solr";
	public static HttpSolrServer solrServer = null;
	private String requiredFieldKey;
	private String requiredFieldValue;

	@Override
	public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
		RecursiveMergeExistingDoc recursiveMergeExistingDoc = new RecursiveMergeExistingDoc(req, rsp, next);
		recursiveMergeExistingDoc.setDelimiter(RecursiveMergeExistingDocFactory.delimiter);
		recursiveMergeExistingDoc.setForeignIdField(this.foreignIdField);
		recursiveMergeExistingDoc.setLocalIdField(this.localIdField);
		recursiveMergeExistingDoc.setForeignSourceFieldList(this.foreignSourceFieldList);
		recursiveMergeExistingDoc.setSolrUrl(this.solrUrl);
		recursiveMergeExistingDoc.setRequiredFieldKey(this.requiredFieldKey);
		recursiveMergeExistingDoc.setRequiredFieldValue(requiredFieldValue);
		return recursiveMergeExistingDoc;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	// Retrieves values from parameters defined in solrconfig.xml
	public void init(NamedList solrConfigParamList) {
		super.init(solrConfigParamList);
		if (solrConfigParamList.get(FOREIGN_SOURCE_FIELD_LIST) != null) {
			this.foreignSourceFieldList = Utilities.getSolrConfigParamListFromParamList(solrConfigParamList, FOREIGN_SOURCE_FIELD_LIST);
		} else {
			throw new SolrException(ErrorCode.FORBIDDEN, "At least one source:destination mapping must be defined in solrconfig.");
		}
		if (solrConfigParamList.get(DELIMITER_PARAM) != null) {
			delimiter = (String) Utilities.getSolrConfigObjectFromParamList(solrConfigParamList, DELIMITER_PARAM);
		}
		this.foreignIdField = (String) Utilities.getSolrConfigObjectFromParamList(solrConfigParamList, FOREIGN_ID_FIELD);
		this.localIdField = (String) Utilities.getSolrConfigObjectFromParamList(solrConfigParamList, LOCAL_ID_FIELD);
		if (solrConfigParamList.get(SOLR_URL) != null) {
			this.solrUrl = (String) Utilities.getSolrConfigObjectFromParamList(solrConfigParamList, SOLR_URL);
		}
		if (solrConfigParamList.get(REQUIRED_FIELD_KEY) != null && solrConfigParamList.get(REQUIRED_FIELD_VALUE) != null) {
			this.requiredFieldKey = (String) Utilities.getSolrConfigObjectFromParamList(solrConfigParamList, REQUIRED_FIELD_KEY);
			this.requiredFieldValue = (String) Utilities.getSolrConfigObjectFromParamList(solrConfigParamList, REQUIRED_FIELD_VALUE);
		} else if ((solrConfigParamList.get(REQUIRED_FIELD_KEY) == null && solrConfigParamList.get(REQUIRED_FIELD_VALUE) != null)
				|| (solrConfigParamList.get(REQUIRED_FIELD_KEY) != null && solrConfigParamList.get(REQUIRED_FIELD_VALUE) == null)) {
			throw new SolrException(ErrorCode.CONFLICT,
					"Both requiredFieldValue and requiredFieldKey must be defined in solrconfig when using this option.");
		}
	}
}
