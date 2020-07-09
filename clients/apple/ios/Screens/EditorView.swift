//
//  EditorView.swift
//  ios_client
//
//  Created by Raayan Pillai on 4/11/20.
//  Copyright © 2020 Lockbook. All rights reserved.
//

import SwiftUI

struct EditorView: View {
    let metadata: FileMetadata
    @State var content: String
    @State private var showingAlert = false
    @EnvironmentObject var coordinator: Coordinator

    var body: some View {
        VStack {
            TextView(text: self.$content)
            VStack(alignment: .leading) {
                Text("id: \(metadata.id)")
                Text("path: \(metadata.parentId)")
                Text("updatedAt: \(intEpochToString(epoch: metadata.contentVersion))")
                Text("version: \(intEpochToString(epoch: metadata.metadataVersion))")
            }
        }
        .alert(isPresented: $showingAlert) {
            Alert(title: Text("Failed to get/update file!"))
        }
        .onAppear {
            if let file = self.coordinator.getFile(id: self.metadata.id) {
                self.content = file.secret
            } else {
                print("Could not load \(self.metadata)")
            }
        }
        .onDisappear {
            if let file = self.coordinator.getFile(id: self.metadata.id) {
                if file.secret != self.content {
                    if (self.coordinator.updateFile(id: self.metadata.id, content: self.content)) {
                        print("Updated \(self.metadata)")
                        self.coordinator.sync()
                    } else {
                        self.showingAlert = true
                    }
                } else {
                    print("Files look the same, not updating")
                }
            }
        }
    }
    
    init(coordinator: Coordinator, metadata: FileMetadata) {
        self.metadata = metadata
        if let file = coordinator.getFile(id: metadata.id) {
            self._content = State.init(initialValue: file.secret)
        } else {
            self._content = State.init(initialValue: "")
        }
    }
}

struct TextView: UIViewRepresentable {
    @Binding var text: String

    func makeCoordinator() -> Coordinator {
        Coordinator(self)
    }

    func makeUIView(context: Context) -> UITextView {

        let myTextView = UITextView()
        myTextView.delegate = context.coordinator

        myTextView.isScrollEnabled = true
        myTextView.isEditable = true
        myTextView.isUserInteractionEnabled = true
        myTextView.backgroundColor = UIColor(white: 0.0, alpha: 0.05)

        return myTextView
    }

    func updateUIView(_ uiView: UITextView, context: Context) {
        uiView.text = text
    }

    class Coordinator : NSObject, UITextViewDelegate {

        var parent: TextView

        init(_ uiTextView: TextView) {
            self.parent = uiTextView
        }

        func textView(_ textView: UITextView, shouldChangeTextIn range: NSRange, replacementText text: String) -> Bool {
            return true
        }

        func textViewDidChange(_ textView: UITextView) {
            self.parent.text = textView.text
        }
    }
}

struct EditorView_Previews: PreviewProvider {
    static var previews: some View {
        NavigationView {
            EditorView(coordinator: Coordinator(), metadata: FakeApi().fakeMetadatas.first!).environmentObject(Coordinator())
        }
    }
}