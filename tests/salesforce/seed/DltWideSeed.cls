// Writes records with random text fields `<prefix>001__c`.. of 255 characters. The org generates the
// values, so no data is uploaded. Seed ids that exist are skipped
public class DltWideSeed implements Database.Batchable<Integer> {
    private final String sobjectName;
    private final String fieldPrefix;
    private final Integer fieldCount;
    private final Integer records;
    private final Integer recordsPerBatch;
    private final Integer firstBatch;
    private final Integer lastBatch;

    public DltWideSeed(
        String sobjectName,
        String fieldPrefix,
        Integer fieldCount,
        Integer records,
        Integer recordsPerBatch,
        Integer firstBatch,
        Integer lastBatch
    ) {
        this.sobjectName = sobjectName;
        this.fieldPrefix = fieldPrefix;
        this.fieldCount = fieldCount;
        this.records = records;
        this.recordsPerBatch = recordsPerBatch;
        this.firstBatch = firstBatch;
        this.lastBatch = lastBatch;
    }

    public Iterable<Integer> start(Database.BatchableContext context) {
        List<Integer> batches = new List<Integer>();
        for (Integer batch = firstBatch; batch < lastBatch; batch++) {
            batches.add(batch);
        }
        return batches;
    }

    public void execute(Database.BatchableContext context, List<Integer> batches) {
        // slices of a random pool larger than the gzip window keep the rows incompressible
        String pool = randomPool(262144);
        Integer maxStart = pool.length() - 255;
        List<String> fields = new List<String>();
        for (Integer f = 1; f <= fieldCount; f++) {
            fields.add(fieldPrefix + String.valueOf(f).leftPad(3, '0') + '__c');
        }
        Schema.SObjectType sobjectType = Schema.getGlobalDescribe().get(sobjectName);
        List<SObject> rows = new List<SObject>();
        for (Integer batch : batches) {
            for (Integer n = batch * recordsPerBatch; n < Math.min((batch + 1) * recordsPerBatch, records); n++) {
                String seedId = 'wide-' + String.valueOf(n).leftPad(6, '0');
                SObject row = sobjectType.newSObject();
                row.put('Name', seedId);
                row.put('Seed_Id__c', seedId);
                for (String field : fields) {
                    Integer start = Math.mod(Math.abs(Crypto.getRandomInteger()), maxStart);
                    row.put(field, pool.substring(start, start + 255));
                }
                rows.add(row);
            }
        }
        // duplicate seed ids fail one by one, so a rerun writes only the missing records
        Database.insert(rows, false);
    }

    public void finish(Database.BatchableContext context) {
    }

    private static String randomPool(Integer chars) {
        List<String> parts = new List<String>();
        Integer length = 0;
        while (length < chars) {
            parts.add(EncodingUtil.base64Encode(Crypto.generateAesKey(256)).substring(0, 43));
            length += 43;
        }
        return String.join(parts, '');
    }
}
