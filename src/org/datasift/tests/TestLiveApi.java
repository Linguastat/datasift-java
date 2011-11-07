/**
 * 
 */
package org.datasift.tests;

import java.util.Date;

import junit.framework.TestCase;

import org.junit.Before;

import org.datasift.Config;
import org.datasift.DPU;
import org.datasift.Definition;
import org.datasift.EAPIError;
import org.datasift.EAccessDenied;
import org.datasift.ECompileFailed;
import org.datasift.EInvalidData;
import org.datasift.User;

/**
 * @author MediaSift
 */
public class TestLiveApi extends TestCase {
	public static void main(String[] args) {
		junit.textui.TestRunner.run(TestLiveApi.class);
	}

	private User user = null;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		user = new User(Config.username, Config.api_key);
	}

	public void testValidate_Success() {
		Definition def = new Definition(user, Config.definition);
		assertEquals("Definition string not set correctly", def.get(),
				Config.definition);

		try {
			def.validate();

			// We should now have a hash
			assertEquals("Incorrect hash", def.getHash(),
					Config.definition_hash);
		} catch (EInvalidData e) {
			fail("InvalidData: " + e.getMessage());
		} catch (ECompileFailed e) {
			fail("CompileFailed: " + e.getMessage());
		} catch (EAccessDenied e) {
			fail("AccessDenied: " + e.getMessage());
		}
	}

	public void testValidate_Failure() {
		Definition def = new Definition(user, Config.invalid_definition);
		assertEquals("Definition string not set correctly", def.get(),
				Config.invalid_definition);

		try {
			def.validate();
			fail("Expected ECompileFailed exception was not thrown");
		} catch (EInvalidData e) {
			fail("InvalidData: " + e.getMessage());
		} catch (ECompileFailed e) {
			// This is what we should get
		} catch (EAccessDenied e) {
			fail("AccessDenied: " + e.getMessage());
		}
	}

	public void testValidate_SuccessThenFailure() {
		Definition def = new Definition(user, Config.definition);
		assertEquals("Definition string not set correctly", def.get(),
				Config.definition);

		try {
			def.validate();

			// We should now have a hash
			assertEquals("Hash is not correct", def.getHash(),
					Config.definition_hash);
		} catch (EInvalidData e) {
			fail("InvalidData: " + e.getMessage());
		} catch (ECompileFailed e) {
			fail("CompileFailed: " + e.getMessage());
		} catch (EAccessDenied e) {
			fail("AccessDenied: " + e.getMessage());
		}

		// Now set the invalid definition in that same object
		def.set(Config.invalid_definition);
		assertEquals("Definition string not set correctly", def.get(),
				Config.invalid_definition);

		try {
			def.compile();
			fail("CompileFailed exception expected, but not thrown");
		} catch (EInvalidData e) {
			fail("InvalidData: " + e.getMessage());
		} catch (ECompileFailed e) {
			// This is what we should get
		} catch (EAccessDenied e) {
			fail("AccessDenied: " + e.getMessage());
		}
	}
	
	public void testCompile_Success() {
		Definition def = new Definition(user, Config.definition);
		assertEquals("Definition string not set correctly", def.get(),
				Config.definition);

		try {
			def.compile();

			// We should now have a hash
			assertEquals("Incorrect hash", def.getHash(),
					Config.definition_hash);
		} catch (EInvalidData e) {
			fail("InvalidData: " + e.getMessage());
		} catch (ECompileFailed e) {
			fail("CompileFailed: " + e.getMessage());
		} catch (EAccessDenied e) {
			fail("AccessDenied: " + e.getMessage());
		}
	}

	public void testCompile_Failure() {
		Definition def = new Definition(user, Config.invalid_definition);
		assertEquals("Definition string not set correctly", def.get(),
				Config.invalid_definition);

		try {
			def.compile();
			fail("Expected ECompileFailed exception was not thrown");
		} catch (EInvalidData e) {
			fail("InvalidData: " + e.getMessage());
		} catch (ECompileFailed e) {
			// This is what we should get
		} catch (EAccessDenied e) {
			fail("AccessDenied: " + e.getMessage());
		}
	}

	public void testCompile_SuccessThenFailure() {
		Definition def = new Definition(user, Config.definition);
		assertEquals("Definition string not set correctly", def.get(),
				Config.definition);

		try {
			def.compile();

			// We should now have a hash
			assertEquals("Hash is not correct", def.getHash(),
					Config.definition_hash);
		} catch (EInvalidData e) {
			fail("InvalidData: " + e.getMessage());
		} catch (ECompileFailed e) {
			fail("CompileFailed: " + e.getMessage());
		} catch (EAccessDenied e) {
			fail("AccessDenied: " + e.getMessage());
		}

		// Now set the invalid definition in that same object
		def.set(Config.invalid_definition);
		assertEquals("Definition string not set correctly", def.get(),
				Config.invalid_definition);

		try {
			def.compile();
			fail("CompileFailed exception expected, but not thrown");
		} catch (EInvalidData e) {
			fail("InvalidData: " + e.getMessage());
		} catch (ECompileFailed e) {
			// This is what we should get
		} catch (EAccessDenied e) {
			fail("AccessDenied: " + e.getMessage());
		}
	}
	
	public void testGetCreatedAt() {
		Definition def = new Definition(user, Config.definition);
		assertEquals("Definition string not set correctly", def.get(),
				Config.definition);
		
		try {
			Date d = def.getCreatedAt();
			assertNotNull(d);
		} catch (EInvalidData e) {
			fail("InvalidData: " + e.getMessage());
		} catch (EAccessDenied e) {
			fail("AccessDenied: " + e.getMessage());
		}
	}
	
	public void testGetTotalDPU() {
		Definition def = new Definition(user, Config.definition);
		assertEquals("Definition string not set correctly", def.get(),
				Config.definition);
		
		try {
			double dpu = def.getTotalDPU();
			assertTrue(dpu > 0);
		} catch (EInvalidData e) {
			fail("InvalidData: " + e.getMessage());
		} catch (EAccessDenied e) {
			fail("AccessDenied: " + e.getMessage());
		}
	}
	
	public void testGetDPUBreakdown() {
		Definition def = new Definition(user, Config.definition);
		assertEquals("Definition string not set correctly", def.get(),
				Config.definition);
		
		try {
			DPU dpu = def.getDPUBreakdown();
			assertEquals(Config.definition_dpu, dpu.getTotal());
		} catch (EInvalidData e) {
			fail("InvalidData: " + e.getMessage());
		} catch (EAccessDenied e) {
			fail("AccessDenied: " + e.getMessage());
		} catch (ECompileFailed e) {
			fail("CompileFailed: " + e.getMessage());
		} catch (EAPIError e) {
			fail("APIError: " + e.getMessage());
		}
	}
	
//	public void testGetUsageSummary() {
//		try {
//			Usage u = user.getUsage();
//
//			assertEquals("Processed count is incorrect", 9999, u.getProcessed());
//			assertEquals("Delivered count is incorrect", 10800, u.getDelivered());
//
//			assertEquals("Processed count for hash a123ab20f37f333824159b8868ad3827 is incorrect", 7505, u.getProcessed("a123ab20f37f333824159b8868ad3827"));
//			assertEquals("Delivered count for hash a123ab20f37f333824159b8868ad3827 is incorrect", 8100, u.getDelivered("a123ab20f37f333824159b8868ad3827"));
//
//			assertEquals("Processed count for hash c369ab20f37f333824159b8868ad3827 is incorrect", 2494, u.getProcessed("c369ab20f37f333824159b8868ad3827"));
//			assertEquals("Delivered count for hash c369ab20f37f333824159b8868ad3827 is incorrect", 2700, u.getDelivered("c369ab20f37f333824159b8868ad3827"));
//
//			int totalProcessed = 0;
//			int totalDelivered = 0;
//			for (String hash : u.getItems()) {
//				totalProcessed += u.getProcessed(hash);
//				totalDelivered += u.getDelivered(hash);
//			}
//			assertEquals("Sum of processed for hashes does not equal total processed", u.getProcessed(), totalProcessed);
//			assertEquals("Sum of delivered for hashes does not equal total delivered", u.getDelivered(), totalDelivered);
//		} catch (EAPIError e) {
//			fail("Caught EAPIError: " + e.toString());
//		} catch (EAccessDenied e) {
//			fail("Caught EAccessDenied: " + e.toString());
//		} catch (EInvalidData e) {
//			fail("Caught EInvalidData: " + e.toString());
//		}
//	}
//
//	public void testGetUsageForStream() {
//		try {
//			Usage u = user.getUsage("a123ab20f37f333824159b8868ad3827");
//
//			assertEquals("Processed count is incorrect", 2494, u.getProcessed());
//			assertEquals("Delivered count is incorrect", 2700, u.getDelivered());
//
//			assertEquals("Processed count for type buzz is incorrect", 247, u.getProcessed("buzz"));
//			assertEquals("Delivered count for type buzz is incorrect", 350, u.getDelivered("buzz"));
//
//			assertEquals("Processed count for type twitter is incorrect", 2247, u.getProcessed("twitter"));
//			assertEquals("Delivered count for type twitter is incorrect", 2350, u.getDelivered("twitter"));
//
//			int totalProcessed = 0;
//			int totalDelivered = 0;
//			for (String hash : u.getItems()) {
//				totalProcessed += u.getProcessed(hash);
//				totalDelivered += u.getDelivered(hash);
//			}
//			assertEquals("Sum of processed for types does not equal total processed", u.getProcessed(), totalProcessed);
//			assertEquals("Sum of delivered for types does not equal total delivered", u.getDelivered(), totalDelivered);
//		} catch (EAPIError e) {
//			fail("Caught EAPIError: " + e.toString());
//		} catch (EAccessDenied e) {
//			fail("Caught EAccessDenied: " + e.toString());
//		} catch (EInvalidData e) {
//			fail("Caught EInvalidData: " + e.toString());
//		}
//	}
}
