package edu.umass.cs.cs646.utils;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.*;

public class LuceneSearchUtils {
	
	/**
	 * A best-match search interface with replacable dd and di functions.
	 */
	public interface BestMatchSearch {
		List<SearchResult> search( IndexReader index, String field, List<String> queryterms, int n, DocumentIndependentWeight di, DocumentDependentWeight dd ) throws IOException;
	}
	
	/**
	 * An interface for implementing document independent term weight (such as IDF).
	 * This score should only be computed once for each posting list.
	 */
	public interface DocumentIndependentWeight {
		
		/**
		 * @param index The Lucene index.
		 * @param field The index field.
		 * @param term  The term.
		 * @return The document independent term weight for scoring.
		 */
		double getWeight( IndexReader index, String field, String term ) throws IOException;
	}
	
	/**
	 * An interface for implementing document dependent term weight (such as TF).
	 * This score should be computed for each entry of the posting list.
	 */
	public interface DocumentDependentWeight {
		
		/**
		 * @param index   The Lucene index.
		 * @param posting The posting list.
		 * @param field   The index field.
		 * @param term    The term.
		 * @return The document dependent term weight for the current posting list entry.
		 */
		double getWeight( IndexReader index, PostingsEnum posting, String field, String term ) throws IOException;
	}
	
	/**
	 * Uniform term weight -- all terms are equally important.
	 */
	public static final class Uniform implements DocumentIndependentWeight {
		public double getWeight( IndexReader index, String field, String term ) throws IOException {
			return 1.0;
		}
	}
	
	/**
	 * The original IDF with 0.5 smoothing.
	 */
	public static final class IDF implements DocumentIndependentWeight {
		
		public double getWeight( IndexReader index, String field, String term ) throws IOException {
			int N = index.numDocs();
			int n = index.docFreq( new Term( field, term ) );
			return (float) Math.log( ( N + 0.5 ) / ( n + 0.5 ) );
		}
	}
	
	/**
	 * Raw TF function.
	 */
	public static final class RawTF implements DocumentDependentWeight {
		
		public double getWeight( IndexReader index, PostingsEnum posting, String field, String term ) throws IOException {
			return posting.freq();
		}
	}
	
	/**
	 * Binary TF function.
	 */
	public static final class BinTF implements DocumentDependentWeight {
		
		public double getWeight( IndexReader index, PostingsEnum posting, String field, String term ) throws IOException {
			return posting.freq() > 0 ? 1 : 0;
		}
	}
	
	/**
	 * Implement your LogTF function here.
	 */
	public static final class LogTF implements DocumentDependentWeight {
		
		protected double base;
		
		public LogTF( double base ) {
			this.base = base;
		}
		
		public double getWeight( IndexReader index, PostingsEnum posting, String field, String term ) throws IOException {
			return 0;
		}
	}
	
	/**
	 * Implement your RSJ function here.
	 */
	public static final class RSJ implements DocumentIndependentWeight {
		
		public double getWeight( IndexReader index, String field, String term ) throws IOException {
			return 0;
		}
	}
	
	/**
	 * Implement your BM25TFUnnormalized function here.
	 */
	public static final class BM25TFUnnormalized implements DocumentDependentWeight {
		
		double k1;
		
		public BM25TFUnnormalized( double k1 ) {
			this.k1 = k1;
		}
		
		public double getWeight( IndexReader index, PostingsEnum posting, String field, String term ) throws IOException {
			return 0;
		}
	}
	
	/**
	 * Implement your term-at-a-time here.
	 */
	public static final class TermAtATime1 implements BestMatchSearch {
		
		public List<SearchResult> search( IndexReader index, String field, List<String> queryterms, int n, DocumentIndependentWeight di, DocumentDependentWeight dd ) throws IOException {
			return null;
		}
	}
	
	/**
	 * Another implementation of term-at-a-time.
	 */
	public static final class TermAtATime2 implements BestMatchSearch {
		
		public List<SearchResult> search( IndexReader index, String field, List<String> queryterms, int n, DocumentIndependentWeight di, DocumentDependentWeight dd ) throws IOException {
			
			Map<String, Integer> queryterms_df = new TreeMap<>();
			List<String> queryterms_filtered = new ArrayList<>();
			for ( String term : queryterms ) {
				int df = index.docFreq( new Term( field, term ) );
				if ( df > 0 ) {
					queryterms_filtered.add( term );
					queryterms_df.put( term, df );
				}
			}
			
			Collections.sort( queryterms_filtered, ( t1, t2 ) -> queryterms_df.get( t1 ) - queryterms_df.get( t2 ) );
			
			int[] docs = new int[0];
			float[] scores = new float[0];
			int length = 0;
			
			for ( String term : queryterms_filtered ) {
				int df = queryterms_df.get( term );
				double di_weight = di.getWeight( index, field, term );
				int[] docs_old = docs;
				float[] scores_old = scores;
				int length_old = length;
				docs = new int[df + length_old];
				scores = new float[df + length_old];
				length = merge(
						MultiFields.getTermDocsEnum( index, field, new BytesRef( term ), PostingsEnum.FREQS ),
						docs_old, scores_old, length_old,
						docs, scores, di_weight,
						index, field, term, dd
				);
			}
			
			PriorityQueue<SearchResult> pq = new PriorityQueue<>( ( o1, o2 ) -> o1.getScore().compareTo( o2.getScore() ) );
			for ( int ix = 0; ix < length; ix++ ) {
				if ( pq.size() < n ) {
					pq.add( new SearchResult( docs[ix], null, scores[ix] ) );
				} else {
					SearchResult result = pq.peek();
					if ( scores[ix] > result.getScore() ) {
						pq.poll();
						pq.add( new SearchResult( docs[ix], null, scores[ix] ) );
					}
				}
			}
			
			List<SearchResult> results = new ArrayList<>( pq.size() );
			results.addAll( pq );
			Collections.sort( results, ( o1, o2 ) -> o2.getScore().compareTo( o1.getScore() ) );
			return results;
		}
		
		private static int merge( PostingsEnum posting, int[] docs_old, float[] scores_old, int length_old, int[] docs, float[] scores, double di_weight, IndexReader index, String field, String term, DocumentDependentWeight dd ) throws IOException {
			int ix = 0;
			int ix_old = 0;
			int doc1 = posting.nextDoc();
			while ( doc1 != PostingsEnum.NO_MORE_DOCS && ix_old < length_old ) {
				int doc2 = docs_old[ix_old];
				if ( doc1 < doc2 ) {
					docs[ix] = doc1;
					scores[ix] += di_weight * dd.getWeight( index, posting, field, term );
					ix++;
					doc1 = posting.nextDoc();
				} else if ( doc1 > doc2 ) {
					docs[ix] = doc2;
					scores[ix] += scores_old[ix_old];
					ix++;
					ix_old++;
				} else {
					docs[ix] = doc1;
					scores[ix] += scores_old[ix_old] + di_weight * dd.getWeight( index, posting, field, term );
					ix++;
					doc1 = posting.nextDoc();
					ix_old++;
				}
			}
			while ( doc1 != PostingsEnum.NO_MORE_DOCS ) {
				docs[ix] = doc1;
				scores[ix] += di_weight * dd.getWeight( index, posting, field, term );
				ix++;
				doc1 = posting.nextDoc();
			}
			while ( ix_old < length_old ) {
				docs[ix] = docs_old[ix_old];
				scores[ix] += scores_old[ix_old];
				ix++;
				ix_old++;
			}
			return ix;
		}
	}
	
	/**
	 * Document-at-a-time.
	 */
	public static final class DocAtATime implements BestMatchSearch {
		
		public List<SearchResult> search( IndexReader index, String field, List<String> queryterms, int n, DocumentIndependentWeight di, DocumentDependentWeight dd ) throws IOException {
			
			PriorityQueue<SearchResult> pq = new PriorityQueue<>( ( o1, o2 ) -> o1.getScore().compareTo( o2.getScore() ) );
			
			List<String> qterms_filtered = new ArrayList<>();
			for ( String term : queryterms ) {
				if ( index.docFreq( new Term( field, term ) ) > 0 ) {
					qterms_filtered.add( term );
				}
			}
			queryterms = qterms_filtered;
			
			int[] cursors = new int[queryterms.size()];
			double[] di_weights = new double[queryterms.size()];
			PostingsEnum[] postings = new PostingsEnum[queryterms.size()];
			for ( int ix = 0; ix < queryterms.size(); ix++ ) {
				postings[ix] = MultiFields.getTermDocsEnum( index, field, new BytesRef( queryterms.get( ix ) ), PostingsEnum.FREQS );
				cursors[ix] = postings[ix].nextDoc();
				di_weights[ix] = di.getWeight( index, field, queryterms.get( ix ) );
			}
			
			while ( true ) {
				int docid_smallest = Integer.MAX_VALUE;
				for ( int cursor : cursors ) {
					if ( cursor >= 0 && cursor < docid_smallest ) {
						docid_smallest = cursor;
					}
				}
				if ( docid_smallest == Integer.MAX_VALUE ) {
					break;
				}
				double score = 0;
				for ( int ix = 0; ix < cursors.length; ix++ ) {
					if ( cursors[ix] == docid_smallest ) {
						score += di_weights[ix] * dd.getWeight( index, postings[ix], field, queryterms.get( ix ) );
						cursors[ix] = postings[ix].nextDoc();
					}
				}
				if ( pq.size() < n ) {
					pq.add( new SearchResult( docid_smallest, null, score ) );
				} else {
					SearchResult result = pq.peek();
					if ( score > result.getScore() ) {
						pq.poll();
						pq.add( new SearchResult( docid_smallest, null, score ) );
					}
				}
			}
			
			List<SearchResult> results = new ArrayList<>( pq.size() );
			results.addAll( pq );
			Collections.sort( results, ( o1, o2 ) -> o2.getScore().compareTo( o1.getScore() ) );
			return results;
		}
	}
	
}
