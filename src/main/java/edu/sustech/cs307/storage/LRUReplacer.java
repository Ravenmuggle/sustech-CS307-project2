package edu.sustech.cs307.storage;

import java.util.*;

public class LRUReplacer {

    private final int maxSize;
    private final Set<Integer> pinnedFrames = new HashSet<>();
    private final Set<Integer> LRUHash = new HashSet<>();
    private final LinkedList<Integer> LRUList = new LinkedList<>();

    public LRUReplacer(int numPages) {
        this.maxSize = numPages;
    }

    public int Victim() {
        if (LRUList.size()>maxSize){
            Iterator<Integer> reverseIterator=LRUList.descendingIterator();
            int FirstDeletable=reverseIterator.next();
            while (pinnedFrames.contains(FirstDeletable){
                FirstDeletable=reverseIterator.next();
                //iterating continuing
            }
            LRUList.removeLastOccurrence(FirstDeletable);
            LRUHash.remove(FirstDeletable);
        }
        else {

        }
        return -1;
    }

    public void Pin(int frameId) {
        if(pinnedFrames.contains(frameId)){
            //doing nothing
        } else if (LRUHash.contains(frameId)) {
            LRUHash.remove(frameId);
            LRUList.removeFirstOccurrence(frameId);
        }
        else{
            pinnedFrames.add(frameId);
        }
    }


    public void Unpin(int frameId) {
        if (pinnedFrames.contains(frameId)){
            LRUHash.add(frameId);
            LRUList.addFirst(frameId);
            Victim();
        }

    }


    public int size() {

        return LRUList.size() + pinnedFrames.size();
    }
}