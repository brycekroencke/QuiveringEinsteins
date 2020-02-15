#https://openproceedings.org/2018/conf/edbt/paper-215.pdf

"""
Input :Queue of unmerged committed tail pages (mergeQ)
Output :Queue of outdated and consolidated base pages to be deallocated
(deallocateQ)
while true do // Step 1
  // wait until the the concurrent merge queue is not empty
  if mergeQ is not empty then // Step 2
    // fetch references to a set of committed tail pages
    batchTailPage <-- mergeQ.dequeue()
    // create a copy of corresponding base pages
    batchConsPage ← batchTailPage.getBasePageCopy()
    decompress(batchConsPage)
    // track if it has seen the latest update of every record
    HashMap seenUpdatesH
    // reading a set of tail pages in reverse order
    // Step 3
    for i = 0; i < batchTailPage.size; i ← i + 1 do tailPage ← batchTailPages[i]
      forj=k−1;j ≥ tailPage.size;j←j−1 do
        record[j] ← j t h record in the tailPage
        RID ← record[j].RID
        if seenUpdatesH does not contain RID then
          seenUpdatesH.add(RID)
          // copy the latest version of record into consolidated pages
          batchConsPage.update(RID, record[j])
        end
        if if all RIDs OR all tail pages are seen then
          compress(batchConsPage) persist(batchConsPage)
          stop examining remaining tail pages
        end
      end
    end
    // Step 4
    // fetch references to the corresponding base pages
    batchBasePage <-- batchTailPage.getBasePageRef()
    // update page directory to point to the consolidated base pages
    PageDirect.swap(batchBasePage, batchConsPage)
    // Step 5
    // queue outdated pages for deallocation once readers prior merge are drained deallocateQ.enqueue(batchBasePage)
  end
end
"""


#### !!!!!! just seudo code turned into mostly python, still a lot of seudo code needs to be changed

while True:
    if len(MergeQ) != 0:
        batchTailPage = mergeQ.pop()
        batchConsPage = batchTailPage.getBasePageCopy()
        decompress(batchConsPage)
        HashMap seenUpdatesH
        for tailPage in batchTailPages:
            for j in (k−1, tailPage.size):
                j −= 1
                record[j] = jth record in the tailPage
                RID = record[j].RID
                if RID not in seenUpdatesH:
                    seenUpdatesH.add(RID)
                    batchConsPage.update(RID, record[j])
                if if all RIDs OR all tail pages are seen then:
                  compress(batchConsPage)
                  persist(batchConsPage)
        batchBasePage = batchTailPage.getBasePageRef()
        PageDirect.swap(batchBasePage, batchConsPage)
        deallocateQ.append(batchBasePage)
