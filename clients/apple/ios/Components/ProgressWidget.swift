//
//  ProgressWidget.swift
//  ios
//
//  Created by Raayan Pillai on 7/5/20.
//  Copyright © 2020 Lockbook. All rights reserved.
//

import SwiftUI

struct ProgressWidget: View {
    @ObservedObject var coordinator: Coordinator
    var height: CGFloat = 5
    
    var body: some View {
        return GeometryReader { geometry in
            self.coordinator.progress.map { prog in
                VStack {
                    ZStack(alignment: .leading) {
                        Rectangle()
                            .frame(width: geometry.size.width, height: self.height)
                            .foregroundColor(.blue)
                            .opacity(0.2)
                        Rectangle()
                            .frame(width: min(geometry.size.width * CGFloat(prog.0), geometry.size.width), height: self.height)
                            .foregroundColor(.blue)
                            .animation(.linear)
                    }.cornerRadius(10)
                    Text(prog.1)
                        .foregroundColor(.blue)
                        .opacity(0.2)
                }
            }
        }
        
    }
}

struct ProgressWidget_Previews: PreviewProvider {
    static var previews: some View {
        ProgressWidget(coordinator: Coordinator())
            .previewLayout(.fixed(width: 300, height: 50))
    }
}